// cache.go —— guild 服务的 Redis 读缓存层(cache-aside,2026-07-12)。
//
// 背景:公会是「全服共享」数据,同一公会的资料(名字 / 会长 / 人数)被全体成员反复读,
// 是典型「多人共享热 key」——单 MySQL 上量后热行 / 热索引会被打爆。按
// docs/design/read-cache-strategy.md §3 P0 给 guild 补 cache-aside 读缓存。
//
// 两类缓存 key(hashtag 括业务 ID,兼容 Redis Cluster / 单元化,§4.6):
//
//	pandora:guild:info:{guild_id}   → guildv1.Guild proto 快照(GetGuild / GetMyGuild 命中)
//	pandora:guild:member:{player_id} → 玩家所属 guild_id 反查(GetMyGuild 先解析归属再取 info)
//
// 一致性(§4):
//   - MySQL 是唯一事实源;缓存 miss / 反序列化失败 → (nil,false,nil) 回落 MySQL,不报错给上层。
//   - 写路径:先写 MySQL 事务,后删缓存(cache-aside 写后删);删失败仅告警,靠短 TTL 兜底。
//   - 只读缓存,不做 write-behind 脏写回,避免与不停服排空冲突(不变量 §16)。
//   - member 反查只缓存「已在某公会」的正向映射,不做负缓存(不在公会不写 key),
//     避免入会后旧「不在公会」负结果长期驻留。
//
// ⚠️ 滚动升级缓存投毒防护(CLAUDE.md §9 不变量 16/17,对齐 data_service/cache.go):
//
//	guild 资料表若在某版本新增列(guilds 加列 + GuildRow 加字段 + guildv1.Guild 加字段 + SELECT
//	加列),滚动升级期新旧副本同时在线:旧副本读库时 SELECT 里没有新列 → 缓存写出「缺新字段」的
//	Guild 快照 → 新副本读到会丢新字段。为此 info 缓存值打「写入方字段号位图」:读取时只信任
//	「写入方字段集 ⊇ 本副本字段集」的条目,否则当未命中回落 MySQL(本副本读库得到自己认得的
//	全部列)。位图对加 / 删字段都敏感(不像「最大字段编号」会漏编号空洞新增与 reserved 删最高号)。
//	member 反查值只是单个 guild_id 标量(受不变量 §9.11 保护,语义不演进),无字段位图,仅魔数 + 定长校验。
package data

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
)

// GuildCache 是 guild 读缓存抽象。biz 只依赖此接口;nil 表示未配置缓存(降级直连 MySQL)。
type GuildCache interface {
	// GetGuild 读公会资料缓存。未命中(含脏 / 反序列化失败 / 滚动升级残缺)→ (nil,false,nil)。
	GetGuild(ctx context.Context, guildID uint64) (*GuildRow, bool, error)
	// SetGuild 回填公会资料缓存(TTL=ttl)。
	SetGuild(ctx context.Context, g *GuildRow, ttl time.Duration) error
	// DelGuild 删公会资料缓存(资料变更 / 解散后调用)。
	DelGuild(ctx context.Context, guildID uint64) error

	// GetMemberGuildID 读玩家所属 guild_id 反查缓存。未命中 → (0,false,nil)。
	GetMemberGuildID(ctx context.Context, playerID uint64) (uint64, bool, error)
	// SetMemberGuildID 回填玩家→guild_id 反查缓存(TTL=ttl)。
	SetMemberGuildID(ctx context.Context, playerID, guildID uint64, ttl time.Duration) error
	// DelMember 删玩家→guild_id 反查缓存(入会 / 退会 / 踢人 / 解散后调用)。
	DelMember(ctx context.Context, playerID uint64) error
}

// ── info 缓存格式:魔数(4B) + 位图长度(big-endian uint32,4B) + 位图 + guildv1.Guild pb ──

// guildInfoMagic 是 info 缓存值魔数,区分「带位图头的新格式」与旧裸 pb / 脏数据。0x01 = 位图格式版本。
var guildInfoMagic = [cacheMagicLen]byte{'P', 'G', 'C', 0x01}

// guildMemberMagic 是 member 反查缓存值魔数;后接 8 字节 big-endian guild_id。
var guildMemberMagic = [cacheMagicLen]byte{'P', 'G', 'M', 0x01}

const (
	cacheMagicLen     = 4
	cacheMaskLenLen   = 4
	cacheHeaderMinLen = cacheMagicLen + cacheMaskLenLen
	memberValueLen    = cacheMagicLen + 8 // 魔数 + 8B guild_id
)

// guildCacheSchemaMask 是本副本 guildv1.Guild 描述符所有字段号的位图(bit n = 字段号 n 存在)。
var guildCacheSchemaMask = computeGuildCacheSchemaMask()

// computeGuildCacheSchemaMask 构造 guildv1.Guild 描述符所有字段号的位图。
func computeGuildCacheSchemaMask() []byte {
	fields := (&guildv1.Guild{}).ProtoReflect().Descriptor().Fields()
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

// writerHasAllReaderFields 判断写入方位图是否 ⊇ 读取方位图(writer 字段集包含 reader 所有置位)。
// 只要读取方有某字段号写入方没有,写入方那条缓存可能缺该列 → 判为不可信,回落 MySQL。
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

func guildInfoKey(guildID uint64) string { return fmt.Sprintf("pandora:guild:info:{%d}", guildID) }
func guildMemberKey(playerID uint64) string {
	return fmt.Sprintf("pandora:guild:member:{%d}", playerID)
}

// guildRowToProto / guildProtoToRow 在存储行与缓存 proto 快照间转换(字段一一对应)。
func guildRowToProto(g *GuildRow) *guildv1.Guild {
	return &guildv1.Guild{
		GuildId:     g.GuildID,
		Name:        g.Name,
		LeaderId:    g.LeaderID,
		MemberCount: g.MemberCount,
		MaxMembers:  g.MaxMembers,
		CreatedMs:   g.CreatedMs,
	}
}

func guildProtoToRow(p *guildv1.Guild) *GuildRow {
	return &GuildRow{
		GuildID:     p.GetGuildId(),
		Name:        p.GetName(),
		LeaderID:    p.GetLeaderId(),
		MemberCount: p.GetMemberCount(),
		MaxMembers:  p.GetMaxMembers(),
		CreatedMs:   p.GetCreatedMs(),
	}
}

// RedisGuildCache 是基于 go-redis/v9 的 GuildCache 实现。
type RedisGuildCache struct {
	rdb redis.UniversalClient
}

// NewRedisGuildCache 构造。
func NewRedisGuildCache(rdb redis.UniversalClient) *RedisGuildCache {
	return &RedisGuildCache{rdb: rdb}
}

func (c *RedisGuildCache) GetGuild(ctx context.Context, guildID uint64) (*GuildRow, bool, error) {
	b, err := c.rdb.Get(ctx, guildInfoKey(guildID)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	// 头部不足 / 魔数不符(旧裸 pb / 脏数据)当未命中回落 MySQL。
	if len(b) < cacheHeaderMinLen || !bytes.Equal(b[:cacheMagicLen], guildInfoMagic[:]) {
		return nil, false, nil
	}
	maskLen := int(binary.BigEndian.Uint32(b[cacheMagicLen:cacheHeaderMinLen]))
	headerLen := cacheHeaderMinLen + maskLen
	if maskLen < 0 || len(b) < headerLen {
		return nil, false, nil
	}
	writerMask := b[cacheHeaderMinLen:headerLen]
	// 写入方字段集必须 ⊇ 本副本字段集,否则条目可能缺本副本认得的列(缓存投毒防护),当未命中。
	if !writerHasAllReaderFields(writerMask, guildCacheSchemaMask) {
		return nil, false, nil
	}
	p := &guildv1.Guild{}
	if err := proto.Unmarshal(b[headerLen:], p); err != nil {
		return nil, false, nil
	}
	// 防御纵深:反序列化出的 guild_id 必须与 key 一致,否则视为脏 / 串号数据。
	if p.GetGuildId() != guildID {
		return nil, false, nil
	}
	return guildProtoToRow(p), true, nil
}

func (c *RedisGuildCache) SetGuild(ctx context.Context, g *GuildRow, ttl time.Duration) error {
	body, err := proto.Marshal(guildRowToProto(g))
	if err != nil {
		return err
	}
	maskLen := len(guildCacheSchemaMask)
	buf := make([]byte, cacheHeaderMinLen+maskLen+len(body))
	copy(buf[:cacheMagicLen], guildInfoMagic[:])
	binary.BigEndian.PutUint32(buf[cacheMagicLen:cacheHeaderMinLen], uint32(maskLen))
	copy(buf[cacheHeaderMinLen:cacheHeaderMinLen+maskLen], guildCacheSchemaMask)
	copy(buf[cacheHeaderMinLen+maskLen:], body)
	return c.rdb.Set(ctx, guildInfoKey(g.GuildID), buf, ttl).Err()
}

func (c *RedisGuildCache) DelGuild(ctx context.Context, guildID uint64) error {
	return c.rdb.Del(ctx, guildInfoKey(guildID)).Err()
}

func (c *RedisGuildCache) GetMemberGuildID(ctx context.Context, playerID uint64) (uint64, bool, error) {
	b, err := c.rdb.Get(ctx, guildMemberKey(playerID)).Bytes()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// 长度 / 魔数不符(旧格式 / 脏数据)当未命中。
	if len(b) != memberValueLen || !bytes.Equal(b[:cacheMagicLen], guildMemberMagic[:]) {
		return 0, false, nil
	}
	guildID := binary.BigEndian.Uint64(b[cacheMagicLen:])
	if guildID == 0 { // 0 非法(不做负缓存),当未命中
		return 0, false, nil
	}
	return guildID, true, nil
}

func (c *RedisGuildCache) SetMemberGuildID(ctx context.Context, playerID, guildID uint64, ttl time.Duration) error {
	if guildID == 0 { // 不缓存「不在公会」负结果
		return nil
	}
	buf := make([]byte, memberValueLen)
	copy(buf[:cacheMagicLen], guildMemberMagic[:])
	binary.BigEndian.PutUint64(buf[cacheMagicLen:], guildID)
	return c.rdb.Set(ctx, guildMemberKey(playerID), buf, ttl).Err()
}

func (c *RedisGuildCache) DelMember(ctx context.Context, playerID uint64) error {
	return c.rdb.Del(ctx, guildMemberKey(playerID)).Err()
}
