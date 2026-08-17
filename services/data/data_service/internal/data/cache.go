// cache.go —— data_service 的 Redis 缓存层(cache-aside,2026-06-16)。
//
// Redis key 模板:pandora:data:player:%d → protobuf bytes(data_service/v1.PlayerData)
//
// 读 miss 回填、写后删除均由 biz 编排;缓存是弱一致旁路:
//   - Get miss / 反序列化失败 → 视为未命中,回落 MySQL,不报错给上层;
//   - Set / Del 失败 → 仅影响命中率,不影响数据正确性(MySQL 才是事实源)。
//
// ⚠️ 零停机滚动升级下的缓存投毒防护(CLAUDE.md §9 不变量 16/17):
//
//	MySQL 侧靠 update_mask 只写掩码列,新副本加的新列不会被旧副本清零。但缓存存的是整条
//	PlayerData pb——旧副本读 MySQL 时,proto2mysql 只会把它「认得的列」读进 PlayerData,
//	新副本刚加的新列(旧副本 proto 描述符里没有)读不进来 → 旧副本得到的是「缺新列的残缺 pd」。
//	若旧副本把这份残缺 pd 直接写进共享缓存,新副本随后读缓存就会拿到「新列丢失」的数据,
//	破坏不停服升级。为此缓存值打上「写入方字段号位图」(writer field-number bitmap):读缓存时
//	只信任「写入方字段集 ⊇ 本副本字段集」的条目;写入方缺任一本副本认得的字段的条目视为未命中,
//	回落 MySQL 由本副本重新读取(本副本读 DB 得到自己认得的全部列),避免读到残缺数据。
//
//	⚠️ 为何不用「最大字段编号」当版本(审核 P1-7 三审):最大编号会漏两类合法演进——
//	  ① 在编号空洞里加字段(如字段集 {1,2,5} 加字段 3,max 仍 5)→ 版本不变,旧副本写的
//	     缺字段 3 的残缺条目骗过版本比较投毒新副本;② reserved 删掉最高编号字段 → max 下降,
//	     版本非单调。改用「字段号位图 + 超集判定」后,任意加/删字段都会改变位图,按集合包含关系
//	     逐位判断,两类演进都能被正确识别。
//
// ⚠️ 格式头(审核 P1):缓存值以 4 字节魔数 `PDC\x02` + 4 字节位图长度(big-endian uint32)+
//
//	位图字节 + PlayerData protobuf 开头。魔数把「无头的旧裸 pb / 脏字节 / 旧 `PDC\x01` 版本格式」
//	与新格式区分开——魔数不符一律当未命中回落 MySQL(滚动升级期新旧格式交叉读只会多打 MySQL,
//	不会崩、不会读到残缺数据)。读取时还会核对反序列化出的 player_id 与 key 一致(串号防御纵深)。
package data

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	plog "github.com/luyuancpp/pandora/pkg/log"
	datav1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/data_service/v1"
)

// cacheSchemaMask 是本副本 PlayerData proto 描述符里「所有字段号」的位图(bit n = 字段号 n 存在)。
// 缓存值以此打标,读取时只信任「写入方位图 ⊇ 本副本位图」的条目,防止旧副本把读库时丢弃了新列的
// 残缺 PlayerData 写进共享缓存后投毒新副本(§9 不变量 16/17)。位图对加/删字段都敏感,不像「最大
// 字段编号」会漏掉编号空洞新增和 reserved 删最高编号(三审 P1-7)。
var cacheSchemaMask = computeCacheSchemaMask()

// cacheMagic 是新格式缓存值的魔数前缀(4 字节),用于区分「带位图头的新格式」与
// 「无头的旧裸 protobuf 字节 / 旧 PDC\x01 版本格式 / 其它脏数据」。审核 P1:此前直接把前 8 字节当
// schema 版本,旧裸 pb(如 player_id=2^42)的头 8 字节可能被误判成高版本,剩余字节又能被 protobuf
// 宽松反序列化成功 → 命中错误数据。加魔数后旧格式一律当未命中回落 MySQL。版本字节 0x02 = 位图格式。
var cacheMagic = [cacheMagicLen]byte{'P', 'D', 'C', 0x02}

// cacheMagicLen 是魔数长度;cacheMaskLenLen 是位图长度字段宽度(big-endian uint32)。
// cacheHeaderMinLen 是「魔数 + 位图长度」的最小头部(不含变长位图)。
const (
	cacheMagicLen     = 4
	cacheMaskLenLen   = 4
	cacheHeaderMinLen = cacheMagicLen + cacheMaskLenLen
)

// computeCacheSchemaMask 构造 PlayerData 描述符所有字段号的位图。bit n 置位表示字段号 n 存在。
func computeCacheSchemaMask() []byte {
	fields := (&datav1.PlayerData{}).ProtoReflect().Descriptor().Fields()
	maxNum := 0
	for i := 0; i < fields.Len(); i++ {
		if n := int(fields.Get(i).Number()); n > maxNum {
			maxNum = n
		}
	}
	mask := make([]byte, maxNum/8+1)
	for i := 0; i < fields.Len(); i++ {
		n := int(fields.Get(i).Number())
		mask[n/8] |= 1 << uint(n%8)
	}
	return mask
}

// writerHasAllReaderFields 判断写入方位图是否包含读取方位图的所有置位(writer 字段集 ⊇ reader 字段集)。
// 只要读取方有某个字段号写入方没有,写入方那条缓存就可能缺该列 → 判为不可信,回落 MySQL。
func writerHasAllReaderFields(writerMask, readerMask []byte) bool {
	for i := 0; i < len(readerMask); i++ {
		var w byte
		if i < len(writerMask) {
			w = writerMask[i]
		}
		if readerMask[i]&^w != 0 { // reader 有置位而 writer 缺
			return false
		}
	}
	return true
}

// cacheKey returns "pandora:data:player:playerID"。
func cacheKey(playerID uint64) string {
	return fmt.Sprintf("pandora:data:player:%d", playerID)
}

// PlayerCache 是玩家数据缓存抽象。
type PlayerCache interface {
	// Get 读缓存。未命中(含反序列化失败)→ (nil, false, nil)。
	Get(ctx context.Context, playerID uint64) (*datav1.PlayerData, bool, error)
	// Set 写缓存(TTL=ttl)。
	Set(ctx context.Context, pd *datav1.PlayerData, ttl time.Duration) error
	// Del 删缓存。
	Del(ctx context.Context, playerID uint64) error
}

// RedisPlayerCache 是基于 go-redis/v9 的 PlayerCache 实现。
type RedisPlayerCache struct {
	rdb redis.UniversalClient
}

// NewRedisPlayerCache 构造。
func NewRedisPlayerCache(rdb redis.UniversalClient) *RedisPlayerCache {
	return &RedisPlayerCache{rdb: rdb}
}

func (c *RedisPlayerCache) Get(ctx context.Context, playerID uint64) (*datav1.PlayerData, bool, error) {
	b, err := c.rdb.Get(ctx, cacheKey(playerID)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		// 缓存读失败不阻断业务,交由上层回落 MySQL。
		return nil, false, err
	}
	// 旧格式 / 脏数据(头部不足)当未命中,回落 MySQL 重填。
	if len(b) < cacheHeaderMinLen {
		return nil, false, nil
	}
	// 魔数不匹配 → 旧裸 protobuf 字节 / 旧 PDC\x01 版本格式 / 其它脏数据,当未命中(不能把头部字节当版本误判)。
	if !bytes.Equal(b[:cacheMagicLen], cacheMagic[:]) {
		return nil, false, nil
	}
	maskLen := int(binary.BigEndian.Uint32(b[cacheMagicLen:cacheHeaderMinLen]))
	headerLen := cacheHeaderMinLen + maskLen
	if maskLen < 0 || len(b) < headerLen {
		// 位图长度越界 → 脏数据,当未命中。
		return nil, false, nil
	}
	writerMask := b[cacheHeaderMinLen:headerLen]
	// 写入方字段集必须 ⊇ 本副本字段集,否则条目可能缺本副本认得的列(缓存投毒防护),当未命中回落 MySQL。
	if !writerHasAllReaderFields(writerMask, cacheSchemaMask) {
		return nil, false, nil
	}
	pd := &datav1.PlayerData{}
	if err := proto.Unmarshal(b[headerLen:], pd); err != nil {
		// 脏缓存当未命中处理(行为不变),但必须留痕:头部/魔数/位图全过了才 Unmarshal 失败
		// = 真坏档,不是滚动升级的旧格式;静默当 miss 只表现为命中率下降,零可查性。
		plog.With(ctx).Warnw("msg", "player_cache_corrupt_entry",
			"player_id", playerID, "reason", "unmarshal_failed", "err", err, "bytes", len(b))
		return nil, false, nil
	}
	// 防御纵深:反序列化出的 player_id 必须与 key 对应的 playerID 一致,否则视为脏 / 串号数据。
	if pd.GetPlayerId() != playerID {
		// 串号是缓存投毒/键错配的强信号,比坏档更严重,必须点名。行为不变(当 miss)。
		plog.With(ctx).Warnw("msg", "player_cache_corrupt_entry",
			"player_id", playerID, "reason", "player_id_mismatch", "cached_player_id", pd.GetPlayerId())
		return nil, false, nil
	}
	return pd, true, nil
}

func (c *RedisPlayerCache) Set(ctx context.Context, pd *datav1.PlayerData, ttl time.Duration) error {
	body, err := proto.Marshal(pd)
	if err != nil {
		return err
	}
	// 头部写入本副本字段号位图,供读方判断写入方字段集是否 ⊇ 自己。
	maskLen := len(cacheSchemaMask)
	buf := make([]byte, cacheHeaderMinLen+maskLen+len(body))
	copy(buf[:cacheMagicLen], cacheMagic[:])
	binary.BigEndian.PutUint32(buf[cacheMagicLen:cacheHeaderMinLen], uint32(maskLen))
	copy(buf[cacheHeaderMinLen:cacheHeaderMinLen+maskLen], cacheSchemaMask)
	copy(buf[cacheHeaderMinLen+maskLen:], body)
	return c.rdb.Set(ctx, cacheKey(pd.GetPlayerId()), buf, ttl).Err()
}

func (c *RedisPlayerCache) Del(ctx context.Context, playerID uint64) error {
	return c.rdb.Del(ctx, cacheKey(playerID)).Err()
}
