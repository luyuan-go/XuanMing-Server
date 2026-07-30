// Package rewardclaim 提供"领奖记录"领域工具:用变长位图(bitmap)记录每个奖励档位
// 是否已领取,判重 O(1)、落地紧凑。对标 mmorpg C++ 侧 RewardClaimSystem(纯工具,
// 不掺业务),Go 无 std::bitset / dynamic_bitset,这里用 []byte 变长位图等价实现。
//
// 两类记录,生命周期不同,刻意分开(详见各自方法注释):
//
//   - 永久类(签到里程碑 / 成就 / 新手 / 永久任务……):按"来源名"各存一条位图,
//     只增不删,bit 位永久稳定。
//   - 活动类:按"活动实例 ID"(每期一个新 ID,含轮次 / 版本)各存一条位图;
//     活动下线时 EraseActivity 删整条,下期新活动用新 ID 从零开始 —— 即使复用了
//     相同的档位 bit 含义也不会串味,因为是 map 里另一条独立位图。
//
// 序列化:位图的规范落地形态就是它的原始 []byte。Snapshot / Load 直接吐出 / 吃入
//
//	map[string][]byte(永久)+ map[uint64][]byte(活动),
//
// 调用方(如 player 服务)把它们填进自己的 proto 存储 record 的 bytes 字段即可,
// 不产生与 proto 漂移的并行 struct。
//
// 并发:本类型非线程安全。它是"每玩家一份"的存档态,由持有方(单 goroutine 或加锁)
// 串行访问,与 mmorpg C++ 侧 component 一致。
package rewardclaim

import (
	"errors"
	"math/bits"
)

// MaxBitIndex 是单条位图允许的 bit 索引安全上界(1,048,576 位 = 128 KiB)。
// 防止恶意 / 错误传入的超大 index 把位图撑到撑爆内存。业务真实档位数远小于此。
const MaxBitIndex = 1 << 20

// ── 条目数上限(CLAUDE.md §9 不变量 18 在 blob 内部的对应物,2026-07-22)──────────
//
// 背景(必读,别再踩):MaxBitIndex 只约束**单条位图多大**,不约束**有多少条位图**。
// 二者是两个独立的失控方向:
//
//	单元素过大 —— 一条位图撑到 128 KiB(已被 MaxBitIndex 挡住)
//	条目数过多 —— map 里塞进任意多条位图(此前完全无闸)
//
// Record 整条序列化后落进 pandora_player.player_reward_claims.record(LONGBLOB,4 GB),
// DB 层等于不设防;而 ClaimReward 是**客户端可直调**的 RPC(Envoy 只要求 JWT,不在 403
// 内部名单内),source / activity_instance_id 又没有配置表白名单——于是客户端每换一个
// source 字符串或 activity_instance_id,就永久新增一条位图。每条最大 128 KiB,
// 约 3.3 万次调用即可把单个玩家的 record 撑到 4 GB;且 ClaimReward 每次都要
// 全量 load→unmarshal→snapshot→marshal→save,record 涨到几 MB 后每次领奖都在
// 读写几 MB,先打爆的是 player 服务与 MySQL 带宽,而不是 4 GB 列上限。
//
// 因此这里补上条目数与来源名长度的硬闸。真正的根治是**配置表白名单**
// (ClaimPermanentByID + BitIndexMap,本文件已提供),奖励配置表落地后应切过去;
// 在那之前,这些上限是唯一的兜底。
const (
	// MaxPermanentSources 是永久类来源种类上限。永久来源来自配置表(签到 / 成就 /
	// 新手 / 永久任务……),是有限枚举,几十种足够;取 64 留足余量。
	MaxPermanentSources = 64
	// MaxActivityInstances 是同时在册的活动实例数上限。活动下线由 EraseActivity 回收,
	// 正常运营同时在线活动远少于此;取 256 留足余量。
	MaxActivityInstances = 256
	// MaxSourceNameLen 是永久来源名的字节长度上限(来源名是配置表里的短标识符)。
	MaxSourceNameLen = 64
)

var (
	// ErrAlreadyClaimed 表示该档位此前已领取(幂等保护)。
	ErrAlreadyClaimed = errors.New("rewardclaim: 该奖励档位已领取")
	// ErrIndexTooLarge 表示 bit 索引超出 MaxBitIndex 安全上界。
	ErrIndexTooLarge = errors.New("rewardclaim: bit 索引超出安全上界")
	// ErrUnknownID 表示业务 ID 不在 BitIndexMap 里(配置表没有此条 / 表已变更)。
	ErrUnknownID = errors.New("rewardclaim: 业务 ID 不在 bit 索引表中")
	// ErrTooManyEntries 表示记录里的位图条目数已达上限,拒绝**新增**条目
	// (已存在的条目不受影响,仍可继续领取)。
	ErrTooManyEntries = errors.New("rewardclaim: 领奖记录条目数超出上限")
	// ErrSourceNameTooLong 表示永久来源名超长。
	ErrSourceNameTooLong = errors.New("rewardclaim: 永久来源名超出长度上限")
)

// BitIndexMap 是"业务 ID → bit 位"的映射,对标 mmorpg C++ 侧读表生成的
// *_table_id_bit_index.h(MissionBitMap / AchievementBitMap……)。
// 业务侧持有自己系统的这张表(由配置表生成),领取 / 查询时按 ID 查出真实 bit 位,
// 而不是直接把业务 ID 当 bit 索引用 —— 这样配置表增删档位时 bit 布局由表统一管控。
type BitIndexMap map[uint64]uint32

// Index 查 id 对应的 bit 位;不存在返回 (0, false)。对标 C++ GetBitIndex。
func (m BitIndexMap) Index(id uint64) (uint32, bool) {
	idx, ok := m[id]
	return idx, ok
}

// bitmap 是变长位图(Go 版 dynamic_bitset),按需向右增长字节。
// 其原始 []byte 即落地形态,无需额外长度字段。
type bitmap struct {
	bits []byte
}

// test 返回索引 i 是否置位;越界(未分配)视为未置位。
func (b *bitmap) test(i uint32) bool {
	byteIdx := i >> 3
	if int(byteIdx) >= len(b.bits) {
		return false
	}
	return b.bits[byteIdx]&(1<<(i&7)) != 0
}

// set 置位索引 i,必要时按需扩容。返回是否成功(超上界则失败)。
func (b *bitmap) set(i uint32) bool {
	if i >= MaxBitIndex {
		return false
	}
	byteIdx := int(i >> 3)
	if byteIdx >= len(b.bits) {
		grown := make([]byte, byteIdx+1)
		copy(grown, b.bits)
		b.bits = grown
	}
	b.bits[byteIdx] |= 1 << (i & 7)
	return true
}

// count 返回已置位的 bit 总数。
func (b *bitmap) count() int {
	c := 0
	for _, by := range b.bits {
		c += bits.OnesCount8(by)
	}
	return c
}

// setIndices 返回所有已置位的 bit 索引(升序)。供组装客户端可见的"已领取列表"。
func (b *bitmap) setIndices() []uint32 {
	out := make([]uint32, 0, b.count())
	for byteIdx, by := range b.bits {
		for bit := 0; by != 0; bit++ {
			if by&1 != 0 {
				out = append(out, uint32(byteIdx)*8+uint32(bit))
			}
			by >>= 1
		}
	}
	return out
}

// trimmed 返回去掉尾部全零字节后的副本,用于落地最小化存储。
func trimmed(src []byte) []byte {
	end := len(src)
	for end > 0 && src[end-1] == 0 {
		end--
	}
	if end == 0 {
		return nil
	}
	out := make([]byte, end)
	copy(out, src[:end])
	return out
}

// Record 是一名玩家完整的领奖状态:永久(按来源名)+ 活动(按活动实例 ID)。
// 用 New 创建。
type Record struct {
	permanent map[string]*bitmap
	activity  map[uint64]*bitmap
}

// New 创建空的领奖记录。
func New() *Record {
	return &Record{
		permanent: make(map[string]*bitmap),
		activity:  make(map[uint64]*bitmap),
	}
}

// permBitmap 取 / 建永久来源位图。
//
// 只在**新增**条目时校验上限:已存在的来源继续领取永不因上限被拒
// (否则调小上限或存量超限会让老玩家领不到已获得的奖励,属回档,违反验收底线第 4 条)。
// Load 进来的存量记录同理不做校验,只是从此不能再长。
func (r *Record) permBitmap(source string) (*bitmap, error) {
	if b := r.permanent[source]; b != nil {
		return b, nil
	}
	if len(source) > MaxSourceNameLen {
		return nil, ErrSourceNameTooLong
	}
	if len(r.permanent) >= MaxPermanentSources {
		return nil, ErrTooManyEntries
	}
	b := &bitmap{}
	r.permanent[source] = b
	return b, nil
}

// actBitmap 取 / 建活动实例位图。上限语义同 permBitmap(只拦新增)。
func (r *Record) actBitmap(instanceID uint64) (*bitmap, error) {
	if b := r.activity[instanceID]; b != nil {
		return b, nil
	}
	if len(r.activity) >= MaxActivityInstances {
		return nil, ErrTooManyEntries
	}
	b := &bitmap{}
	r.activity[instanceID] = b
	return b, nil
}

// ── 永久类 ───────────────────────────────────────────────────────────────

// ClaimPermanent 领取永久来源 source 的第 index 档奖励。
// 返回 nil 表示首次领取成功;ErrAlreadyClaimed 表示已领过(幂等);
// ErrIndexTooLarge 表示索引超界;ErrTooManyEntries / ErrSourceNameTooLong 表示
// 要新建的来源条目触碰上限(已有来源不受影响)。各 source 互相独立,只增不删。
func (r *Record) ClaimPermanent(source string, index uint32) error {
	if index >= MaxBitIndex {
		return ErrIndexTooLarge
	}
	b, err := r.permBitmap(source)
	if err != nil {
		return err
	}
	if b.test(index) {
		return ErrAlreadyClaimed
	}
	b.set(index)
	return nil
}

// IsPermanentClaimed 查询永久来源 source 的第 index 档是否已领取。
func (r *Record) IsPermanentClaimed(source string, index uint32) bool {
	b := r.permanent[source]
	return b != nil && b.test(index)
}

// PermanentCount 返回永久来源 source 已领取的档位数。
func (r *Record) PermanentCount(source string) int {
	b := r.permanent[source]
	if b == nil {
		return 0
	}
	return b.count()
}

// PermanentClaimedIndices 返回永久来源 source 已领取的所有 bit 索引(升序)。
// 用于组装客户端可见的"已领取列表"最小视图(不外露原始位图)。
func (r *Record) PermanentClaimedIndices(source string) []uint32 {
	b := r.permanent[source]
	if b == nil {
		return nil
	}
	return b.setIndices()
}

// ClaimPermanentByID 按业务 ID 领取永久来源 source 的奖励:先用 bitMap 把 id 查成
// bit 位再领取。对标 C++ SetBit(MissionBitMap, bits, missionId)。
// id 不在 bitMap 里返回 ErrUnknownID;其余语义同 ClaimPermanent。
func (r *Record) ClaimPermanentByID(source string, bitMap BitIndexMap, id uint64) error {
	idx, ok := bitMap.Index(id)
	if !ok {
		return ErrUnknownID
	}
	return r.ClaimPermanent(source, idx)
}

// IsPermanentClaimedByID 按业务 ID 查询永久来源 source 是否已领取。
// 对标 C++ TestBit(MissionBitMap, bits, missionId)。id 不在表中视为未领取。
func (r *Record) IsPermanentClaimedByID(source string, bitMap BitIndexMap, id uint64) bool {
	idx, ok := bitMap.Index(id)
	if !ok {
		return false
	}
	return r.IsPermanentClaimed(source, idx)
}

// ── 活动类 ───────────────────────────────────────────────────────────────

// ClaimActivity 领取活动实例 instanceID 的第 index 档奖励。
// instanceID 应是"每期唯一"的活动实例 ID(含轮次 / 版本),不要用会被复用的配置 ID。
// 返回值语义同 ClaimPermanent。
func (r *Record) ClaimActivity(instanceID uint64, index uint32) error {
	if index >= MaxBitIndex {
		return ErrIndexTooLarge
	}
	b, err := r.actBitmap(instanceID)
	if err != nil {
		return err
	}
	if b.test(index) {
		return ErrAlreadyClaimed
	}
	b.set(index)
	return nil
}

// IsActivityClaimed 查询活动实例 instanceID 的第 index 档是否已领取。
func (r *Record) IsActivityClaimed(instanceID uint64, index uint32) bool {
	b := r.activity[instanceID]
	return b != nil && b.test(index)
}

// ClaimActivityByID 按业务 ID 领取活动实例 instanceID 的奖励:先用 bitMap 把 id 查成
// bit 位再领取。活动配置同样有自己的"档位 ID → bit 位"表。
// id 不在 bitMap 里返回 ErrUnknownID;其余语义同 ClaimActivity。
func (r *Record) ClaimActivityByID(instanceID uint64, bitMap BitIndexMap, id uint64) error {
	idx, ok := bitMap.Index(id)
	if !ok {
		return ErrUnknownID
	}
	return r.ClaimActivity(instanceID, idx)
}

// IsActivityClaimedByID 按业务 ID 查询活动实例 instanceID 是否已领取。
// id 不在表中视为未领取。
func (r *Record) IsActivityClaimedByID(instanceID uint64, bitMap BitIndexMap, id uint64) bool {
	idx, ok := bitMap.Index(id)
	if !ok {
		return false
	}
	return r.IsActivityClaimed(instanceID, idx)
}

// ActivityCount 返回活动实例 instanceID 已领取的档位数。
func (r *Record) ActivityCount(instanceID uint64) int {
	b := r.activity[instanceID]
	if b == nil {
		return 0
	}
	return b.count()
}

// ActivityClaimedIndices 返回活动实例 instanceID 已领取的所有 bit 索引(升序)。
// 用于组装客户端可见的"已领取列表"最小视图。
func (r *Record) ActivityClaimedIndices(instanceID uint64) []uint32 {
	b := r.activity[instanceID]
	if b == nil {
		return nil
	}
	return b.setIndices()
}

// HasActivity 返回是否存在活动实例 instanceID 的记录。
func (r *Record) HasActivity(instanceID uint64) bool {
	_, ok := r.activity[instanceID]
	return ok
}

// EraseActivity 删除活动实例 instanceID 的整条记录(活动下线回收)。
// 删除后内存与落地都不再保留该实例的任何 bit;下期新活动用新 instanceID 从零开始,
// 天然解决档位复用污染。返回是否确有该条被删除。
func (r *Record) EraseActivity(instanceID uint64) bool {
	if _, ok := r.activity[instanceID]; !ok {
		return false
	}
	delete(r.activity, instanceID)
	return true
}

// EraseActivities 批量删除指定的一批活动实例,返回实际删除的条数。
// 用于一次性清掉已知下线的多个活动。
func (r *Record) EraseActivities(instanceIDs []uint64) int {
	removed := 0
	for _, id := range instanceIDs {
		if _, ok := r.activity[id]; ok {
			delete(r.activity, id)
			removed++
		}
	}
	return removed
}

// RetainActivities 只保留 activeIDs 中仍然有效的活动实例,删除其余所有活动记录,
// 返回被清理掉的条数。这是活动过期回收的主入口:上层在玩家上线 / 存档前,
// 传入"当前仍开启的活动实例 ID 集合",一次性把所有已下线活动的位图整段清空,
// 直接缩小玩家存档体积。activeIDs 为空表示清空全部活动记录。
func (r *Record) RetainActivities(activeIDs map[uint64]struct{}) int {
	removed := 0
	for id := range r.activity {
		if _, keep := activeIDs[id]; !keep {
			delete(r.activity, id)
			removed++
		}
	}
	return removed
}

// ActivityIDs 返回当前持有记录的所有活动实例 ID(顺序不定)。
// 供上层与"仍有效活动集合"比对,决定清理哪些。
func (r *Record) ActivityIDs() []uint64 {
	ids := make([]uint64, 0, len(r.activity))
	for id := range r.activity {
		ids = append(ids, id)
	}
	return ids
}

// ── 序列化(落地 proto bytes)──────────────────────────────────────────────

// Snapshot 导出落地形态:永久 map[来源名]位图字节 + 活动 map[实例ID]位图字节。
// 字节已去掉尾部全零(最小化存储);全空的条目不会出现在结果里。
// 调用方把这两张 map 填进自己的 proto 存储 record(map<string,bytes> /
// map<uint64,bytes>)即可。
func (r *Record) Snapshot() (permanent map[string][]byte, activity map[uint64][]byte) {
	permanent = make(map[string][]byte, len(r.permanent))
	for src, b := range r.permanent {
		if t := trimmed(b.bits); t != nil {
			permanent[src] = t
		}
	}
	activity = make(map[uint64][]byte, len(r.activity))
	for id, b := range r.activity {
		if t := trimmed(b.bits); t != nil {
			activity[id] = t
		}
	}
	return permanent, activity
}

// Load 从落地形态重建 Record。入参可为 nil。会对每段字节做防御性拷贝,
// 重建后的 Record 不与入参共享底层数组。
func Load(permanent map[string][]byte, activity map[uint64][]byte) *Record {
	r := New()
	for src, raw := range permanent {
		if t := trimmed(raw); t != nil {
			r.permanent[src] = &bitmap{bits: t}
		}
	}
	for id, raw := range activity {
		if t := trimmed(raw); t != nil {
			r.activity[id] = &bitmap{bits: t}
		}
	}
	return r
}
