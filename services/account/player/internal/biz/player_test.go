// player_test.go — PlayerUsecase 业务逻辑单测(W4 ④,2026-06-06)。
//
// 用内存版 fakeRepo 复刻 MySQL 幂等 / clamp / 战绩计数语义,无需真 DB。
package biz

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// fakeProfile 是内存玩家档案。
type fakeProfile struct {
	nickname     string
	mmr          int
	totalBattles int32
	totalWins    int32
}

// fakeRepo 是 data.PlayerRepo 的内存实现(复刻 MySQL 幂等语义)。
type fakeRepo struct {
	players      map[uint64]*fakeProfile
	heroes       map[uint64]map[uint32]bool
	idem         map[string]int // key=playerID|idempotencyKey → 已记录 new_mmr
	activeHero   map[uint64]uint32
	attrs        map[uint64]map[string]int32
	unspent      map[uint64]int
	grants       map[string]bool // key=playerID|idempotencyKey
	equipment    map[uint64][]data.EquipmentSlot
	talents      map[uint64]map[uint32]data.TalentLevel
	skillCards   map[uint64]map[uint32]data.SkillCard
	skillSlots   map[uint64]map[uint32]uint32 // playerID → slot → cardID
	cardGrants   map[string]bool              // key=playerID|idempotencyKey
	talentTotal  map[uint64]int
	talentGrants map[string]bool   // key=playerID|idempotencyKey
	rewardRec    map[uint64][]byte // 领奖记录序列化 bytes
	rewardVer    map[uint64]int32  // 领奖记录乐观锁版本

	// 玩家等级经验(实时成长):level/exp 存在 expLevel/expInLevel,幂等键在 expIdem,
	// 推送出箱行按序追加(复刻 MySQL 同事务出箱语义)。
	expLevel   map[uint64]int32
	expInLevel map[uint64]uint64
	expIdem    map[string]bool // key=playerID|idempotencyKey
	pushOutbox []data.PushOutboxRecord
	pushNextID int64
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		players:      map[uint64]*fakeProfile{},
		heroes:       map[uint64]map[uint32]bool{},
		idem:         map[string]int{},
		activeHero:   map[uint64]uint32{},
		attrs:        map[uint64]map[string]int32{},
		unspent:      map[uint64]int{},
		grants:       map[string]bool{},
		equipment:    map[uint64][]data.EquipmentSlot{},
		talents:      map[uint64]map[uint32]data.TalentLevel{},
		skillCards:   map[uint64]map[uint32]data.SkillCard{},
		skillSlots:   map[uint64]map[uint32]uint32{},
		cardGrants:   map[string]bool{},
		talentTotal:  map[uint64]int{},
		talentGrants: map[string]bool{},
		rewardRec:    map[uint64][]byte{},
		rewardVer:    map[uint64]int32{},
		expLevel:     map[uint64]int32{},
		expInLevel:   map[uint64]uint64{},
		expIdem:      map[string]bool{},
	}
}

func (f *fakeRepo) EnsureProfile(_ context.Context, playerID uint64, defaultNickname string, baseMMR int) error {
	if _, ok := f.players[playerID]; !ok {
		f.players[playerID] = &fakeProfile{nickname: defaultNickname, mmr: baseMMR}
	}
	return nil
}

func (f *fakeRepo) GetProfile(_ context.Context, playerID uint64) (*playerv1.PlayerProfile, bool, error) {
	p, ok := f.players[playerID]
	if !ok {
		return nil, false, nil
	}
	return &playerv1.PlayerProfile{
		PlayerId:     playerID,
		Nickname:     p.nickname,
		Mmr:          int32(p.mmr),
		TotalBattles: p.totalBattles,
		TotalWins:    p.totalWins,
	}, true, nil
}

func (f *fakeRepo) UpdateNickname(_ context.Context, playerID uint64, nickname string) error {
	for pid, p := range f.players {
		if pid != playerID && p.nickname == nickname {
			return errcode.New(errcode.ErrPlayerNicknameTaken, "taken")
		}
	}
	p, ok := f.players[playerID]
	if !ok {
		return errcode.New(errcode.ErrPlayerNotFound, "not found")
	}
	p.nickname = nickname
	return nil
}

func (f *fakeRepo) ListHeroes(_ context.Context, playerID uint64) ([]uint32, error) {
	var out []uint32
	for h := range f.heroes[playerID] {
		out = append(out, h)
	}
	return out, nil
}

func (f *fakeRepo) UnlockHero(_ context.Context, playerID uint64, heroID uint32, _ string) (bool, error) {
	if f.heroes[playerID] == nil {
		f.heroes[playerID] = map[uint32]bool{}
	}
	if f.heroes[playerID][heroID] {
		return true, nil
	}
	f.heroes[playerID][heroID] = true
	return false, nil
}

func (f *fakeRepo) GetMMR(_ context.Context, playerID uint64) (int, bool, error) {
	p, ok := f.players[playerID]
	if !ok {
		return 0, false, nil
	}
	return p.mmr, true, nil
}

func (f *fakeRepo) ApplyMMRChange(_ context.Context, c data.MMRChange) (int, bool, error) {
	p, ok := f.players[c.PlayerID]
	if !ok {
		return 0, false, errcode.New(errcode.ErrPlayerNotFound, "not found")
	}
	idemKey := keyOf(c.PlayerID, c.IdempotencyKey)
	if recorded, hit := f.idem[idemKey]; hit {
		return recorded, true, nil
	}
	newMMR := p.mmr + int(c.Delta)
	if newMMR < c.Floor {
		newMMR = c.Floor
	}
	p.mmr = newMMR
	if c.IncBattle {
		p.totalBattles++
	}
	if c.IncWin {
		p.totalWins++
	}
	f.idem[idemKey] = newMMR
	return newMMR, false, nil
}

func keyOf(pid uint64, k string) string {
	return strconv.FormatUint(pid, 10) + "|" + k
}

func (f *fakeRepo) IsHeroOwned(_ context.Context, playerID uint64, heroID uint32) (bool, error) {
	return f.heroes[playerID][heroID], nil
}

func (f *fakeRepo) SetActiveHero(_ context.Context, playerID uint64, heroID uint32) error {
	f.activeHero[playerID] = heroID
	return nil
}

func (f *fakeRepo) GetActiveHero(_ context.Context, playerID uint64) (uint32, error) {
	return f.activeHero[playerID], nil
}

func (f *fakeRepo) GrantAttributePoints(_ context.Context, playerID uint64, points int32, idempotencyKey string) (int, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	if f.grants[gk] {
		return f.unspent[playerID], true, nil
	}
	f.grants[gk] = true
	f.unspent[playerID] += int(points)
	return f.unspent[playerID], false, nil
}

func (f *fakeRepo) AllocateAttributePoints(_ context.Context, playerID uint64, allocs []data.AttrAllocation) (int, error) {
	// 复刻 MySQL repo 的溢出安全自守:归并增量 checked-add,校验单键列上界 / 总和 / unspent。
	perKey := make(map[string]int64, len(allocs))
	var sum int64
	for _, a := range allocs {
		if a.Key == "" {
			return 0, errcode.New(errcode.ErrInvalidArg, "attr_key must not be empty")
		}
		if a.Points <= 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "points must be positive")
		}
		perKey[a.Key] += int64(a.Points)
		if perKey[a.Key] > math.MaxInt32 {
			return 0, errcode.New(errcode.ErrInvalidArg, "attr allocation out of range")
		}
		sum += int64(a.Points)
		if sum > math.MaxInt32 {
			return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "total out of range")
		}
	}
	if sum > int64(f.unspent[playerID]) {
		return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "insufficient")
	}
	if f.attrs[playerID] == nil {
		f.attrs[playerID] = map[string]int32{}
	}
	for k, delta := range perKey {
		if int64(f.attrs[playerID][k])+delta > math.MaxInt32 {
			return 0, errcode.New(errcode.ErrInvalidArg, "attr cumulative out of range")
		}
	}
	for k, delta := range perKey {
		f.attrs[playerID][k] += int32(delta)
	}
	f.unspent[playerID] -= int(sum)
	return f.unspent[playerID], nil
}

func (f *fakeRepo) ResetAttributes(_ context.Context, playerID uint64) (int, error) {
	var total int32
	for _, p := range f.attrs[playerID] {
		total += p
	}
	f.attrs[playerID] = map[string]int32{}
	f.unspent[playerID] += int(total)
	return f.unspent[playerID], nil
}

func (f *fakeRepo) GetAttributes(_ context.Context, playerID uint64) ([]data.AttrPoint, int, error) {
	var out []data.AttrPoint
	for k, p := range f.attrs[playerID] {
		out = append(out, data.AttrPoint{Key: k, Points: p})
	}
	return out, f.unspent[playerID], nil
}

func (f *fakeRepo) SetEquipment(_ context.Context, playerID uint64, slots []data.EquipmentSlot) error {
	cp := make([]data.EquipmentSlot, len(slots))
	copy(cp, slots)
	f.equipment[playerID] = cp
	return nil
}

func (f *fakeRepo) GetEquipment(_ context.Context, playerID uint64) ([]data.EquipmentSlot, error) {
	return f.equipment[playerID], nil
}

// talentUsed 复刻 MySQL 的 SUM(spent_points) 口径:已花点数按落库的每节点实际消耗算,
// **不是** Σ 等级。用等级和会让 cost_per_level≠1 的分配读出虚高的可点数(写扣 6 读算 4)。
func (f *fakeRepo) talentUsed(playerID uint64) int {
	var used int
	for _, t := range f.talents[playerID] {
		used += int(t.SpentPoints)
	}
	return used
}

func (f *fakeRepo) GrantTalentPoints(_ context.Context, playerID uint64, points int32, idempotencyKey string) (int, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	if f.talentGrants[gk] {
		return f.talentTotal[playerID] - f.talentUsed(playerID), true, nil
	}
	f.talentGrants[gk] = true
	f.talentTotal[playerID] += int(points)
	return f.talentTotal[playerID] - f.talentUsed(playerID), false, nil
}

// SetTalents 复刻 MySQL 语义:总消耗 = Σ 每节点 SpentPoints(biz 按专精表算好填入),
// 每节点消耗随分配一起落库,读取侧据此还原已花点数——repo 层看不到配置表,
// 自行按 sum(level) 推算会在 cost_per_level≠1 时算少扣。
func (f *fakeRepo) SetTalents(_ context.Context, playerID uint64, talents []data.TalentLevel) (int, error) {
	var totalCost int64
	for _, t := range talents {
		if t.SpentPoints <= 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "talent %d missing spent points", t.TalentID)
		}
		totalCost += int64(t.SpentPoints)
	}
	if totalCost > int64(f.talentTotal[playerID]) {
		return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "insufficient")
	}
	m := map[uint32]data.TalentLevel{}
	for _, t := range talents {
		m[t.TalentID] = t
	}
	f.talents[playerID] = m
	return f.talentTotal[playerID] - int(totalCost), nil
}

func (f *fakeRepo) ResetTalents(_ context.Context, playerID uint64) (int, error) {
	f.talents[playerID] = map[uint32]data.TalentLevel{}
	return f.talentTotal[playerID], nil
}

func (f *fakeRepo) GetTalents(_ context.Context, playerID uint64) ([]data.TalentLevel, int, error) {
	var out []data.TalentLevel
	for _, t := range f.talents[playerID] {
		out = append(out, t)
	}
	return out, f.talentTotal[playerID] - f.talentUsed(playerID), nil
}

// ── 技能卡(持有 / 培养 / 更换)────────────────────────────────────────────────

// GrantSkillCards 复刻 MySQL 的 ON DUPLICATE KEY UPDATE 语义:已持有累加碎片、等级不动;
// 未持有以 1 级建卡。命中幂等键一片碎片都不加。
func (f *fakeRepo) GrantSkillCards(_ context.Context, playerID uint64, grants []data.SkillCardGrant, idempotencyKey string) ([]data.SkillCard, bool, error) {
	gk := fmt.Sprintf("%d|%s", playerID, idempotencyKey)
	if f.cardGrants[gk] {
		return f.skillCardsOf(playerID), true, nil
	}
	f.cardGrants[gk] = true
	if f.skillCards[playerID] == nil {
		f.skillCards[playerID] = map[uint32]data.SkillCard{}
	}
	for _, g := range grants {
		card, ok := f.skillCards[playerID][g.CardID]
		if !ok {
			card = data.SkillCard{CardID: g.CardID, Level: 1}
		}
		card.Shards += g.Shards
		f.skillCards[playerID][g.CardID] = card
	}
	return f.skillCardsOf(playerID), false, nil
}

// UpgradeSkillCard 复刻事务语义:价钱按**当前**等级查曲线(不由调用方预先算好),
// 满级 / 碎片不足 / 曲线断档各自返回对应错误。
func (f *fakeRepo) UpgradeSkillCard(_ context.Context, playerID uint64, cardID uint32, costByLevel map[uint32]uint32, maxLevel uint32) (data.SkillCard, uint32, error) {
	card, ok := f.skillCards[playerID][cardID]
	if !ok {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrSkillCardNotOwned, "not owned")
	}
	if card.Level >= maxLevel {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrSkillCardMaxLevel, "max level")
	}
	target := card.Level + 1
	cost, found := costByLevel[target]
	if !found {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrInternal, "curve missing level %d", target)
	}
	if card.Shards < cost {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrSkillCardInsufficientShards, "insufficient shards")
	}
	card.Level = target
	card.Shards -= cost
	f.skillCards[playerID][cardID] = card
	return card, cost, nil
}

func (f *fakeRepo) SetSkillSlots(_ context.Context, playerID uint64, slots []data.SkillSlot) error {
	for _, s := range slots {
		if _, ok := f.skillCards[playerID][s.CardID]; !ok {
			return errcode.New(errcode.ErrSkillCardNotOwned, "not owned: %d", s.CardID)
		}
	}
	m := map[uint32]uint32{}
	for _, s := range slots {
		m[s.Slot] = s.CardID
	}
	f.skillSlots[playerID] = m
	return nil
}

func (f *fakeRepo) GetSkillCards(_ context.Context, playerID uint64) ([]data.SkillCard, error) {
	return f.skillCardsOf(playerID), nil
}

func (f *fakeRepo) GetSkillSlots(_ context.Context, playerID uint64) ([]data.SkillSlot, error) {
	slots := make([]data.SkillSlot, 0, len(f.skillSlots[playerID]))
	for slot, cardID := range f.skillSlots[playerID] {
		slots = append(slots, data.SkillSlot{Slot: slot, CardID: cardID})
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].Slot < slots[j].Slot })
	return slots, nil
}

// skillCardsOf 按 card_id 定序返回持卡,与 SQL 的 ORDER BY card_id 一致——
// map 遍历顺序不稳定会让断言随机失败。
func (f *fakeRepo) skillCardsOf(playerID uint64) []data.SkillCard {
	cards := make([]data.SkillCard, 0, len(f.skillCards[playerID]))
	for _, c := range f.skillCards[playerID] {
		cards = append(cards, c)
	}
	sort.Slice(cards, func(i, j int) bool { return cards[i].CardID < cards[j].CardID })
	return cards
}

// ── 玩家等级经验(实时成长)──────────────────────────────────────────────────

func (f *fakeRepo) ApplyExperience(_ context.Context, apply data.ExpApply) (data.ExpState, bool, error) {
	if _, ok := f.players[apply.PlayerID]; !ok {
		return data.ExpState{}, false, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", apply.PlayerID)
	}
	maxLevel := int32(len(apply.Curve)) + 1
	level := f.expLevel[apply.PlayerID]
	if level < 1 {
		level = 1
	}
	exp := f.expInLevel[apply.PlayerID]
	key := fmt.Sprintf("%d|%s", apply.PlayerID, apply.IdempotencyKey)
	// 满级 no-op:不加经验、不出箱,但仍消费幂等键落 no-op 收据;重放命中收据
	// 返回 already=true(复刻 MySQL 实现,审计 P2 契约)。
	if level >= maxLevel {
		already := f.expIdem[key]
		f.expIdem[key] = true
		return data.ExpState{Level: maxLevel, ExpInLevel: 0, IsMaxLevel: true}, already, nil
	}
	if f.expIdem[key] {
		return data.ExpState{Level: level, ExpInLevel: exp, IsMaxLevel: level >= maxLevel}, true, nil
	}
	f.expIdem[key] = true
	newLevel, newExp, gained := data.AdvanceExperience(level, exp, apply.Delta, apply.Curve)
	f.expLevel[apply.PlayerID] = newLevel
	f.expInLevel[apply.PlayerID] = newExp
	evt := &playerv1.PlayerExperienceEvent{
		PlayerId: apply.PlayerID, Level: newLevel, ExpInLevel: newExp,
		IsMaxLevel: newLevel >= maxLevel, LevelsGained: gained,
	}
	payload, err := proto.Marshal(evt)
	if err != nil {
		return data.ExpState{}, false, err
	}
	f.pushNextID++
	f.pushOutbox = append(f.pushOutbox, data.PushOutboxRecord{
		ID:        f.pushNextID,
		PlayerID:  apply.PlayerID,
		EventType: uint32(playerv1.PlayerPushEventType_PLAYER_PUSH_EVENT_TYPE_EXPERIENCE),
		Payload:   payload,
	})
	return data.ExpState{Level: newLevel, ExpInLevel: newExp, IsMaxLevel: newLevel >= maxLevel, LevelsGained: gained}, false, nil
}

func (f *fakeRepo) FetchPushOutbox(_ context.Context, limit int) ([]data.PushOutboxRecord, error) {
	if limit <= 0 || limit > len(f.pushOutbox) {
		limit = len(f.pushOutbox)
	}
	out := make([]data.PushOutboxRecord, limit)
	copy(out, f.pushOutbox[:limit])
	return out, nil
}

func (f *fakeRepo) DeletePushOutbox(_ context.Context, id int64) error {
	for i, rec := range f.pushOutbox {
		if rec.ID == id {
			f.pushOutbox = append(f.pushOutbox[:i], f.pushOutbox[i+1:]...)
			return nil
		}
	}
	return nil
}

func (f *fakeRepo) SweepExpHistory(_ context.Context, mode dbguard.Mode, _ time.Time, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

// 保留期清理(§9.24):biz 单测不模拟时间,默认 no-op。
// 模式语义(report_only 不循环 / delete 追平积压)由 retention_test.go 直接测 drainRetention。
func (f *fakeRepo) SweepMMRHistory(_ context.Context, mode dbguard.Mode, _ time.Time, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) SweepAttrPointGrants(_ context.Context, mode dbguard.Mode, _ time.Time, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) SweepTalentPointGrants(_ context.Context, mode dbguard.Mode, _ time.Time, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) SweepSkillCardGrants(_ context.Context, mode dbguard.Mode, _ time.Time, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) LoadRewardClaims(_ context.Context, playerID uint64) ([]byte, int32, error) {
	return f.rewardRec[playerID], f.rewardVer[playerID], nil
}

func (f *fakeRepo) SaveRewardClaims(_ context.Context, playerID uint64, record []byte, expectVersion int32) error {
	if f.rewardVer[playerID] != expectVersion {
		return errcode.New(errcode.ErrPlayerVersionMismatch, "reward claims player=%d version mismatch", playerID)
	}
	f.rewardRec[playerID] = record
	f.rewardVer[playerID] = expectVersion + 1
	return nil
}

func newUC(repo data.PlayerRepo) *PlayerUsecase {
	return NewPlayerUsecase(repo, conf.PlayerConf{BaseMMR: 1500, MMRFloor: 0, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32})
}

func newUCHero(repo data.PlayerRepo) *PlayerUsecase {
	return NewPlayerUsecase(repo, conf.PlayerConf{BaseMMR: 1500, MMRFloor: 0, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32, HeroSelectionEnabled: true})
}

// stubItemRules 是道具表判定的测试替身:itemConfigID → 装备部位(0 / 缺失 = 不可穿戴)。
type stubItemRules struct {
	slotByItem map[uint32]uint32
}

func (s stubItemRules) MatchesSlot(itemConfigID uint32, slot uint32) bool {
	if slot == 0 {
		return false
	}
	got, ok := s.slotByItem[itemConfigID]
	return ok && got == slot
}

// stubTalentNode 是专精表一行的测试替身。
type stubTalentNode struct {
	maxLevel uint32
	cost     uint32
	reqID    uint32
	reqLevel uint32
}

// stubTalentRules 复刻 configtable.TalentTable.ValidateAllocation 的判定口径
// (节点存在 / 不超上限 / 前置在本次方案内达标 / 总消耗 = Σ 等级 × 每级消耗)。
type stubTalentRules struct {
	nodes map[uint32]stubTalentNode
}

func (s stubTalentRules) ValidateAllocation(levels map[uint32]uint32) (map[uint32]uint32, uint32, error) {
	var total uint32
	costs := make(map[uint32]uint32, len(levels))
	for id, lv := range levels {
		node, ok := s.nodes[id]
		if !ok {
			return nil, 0, fmt.Errorf("专精 %d 不在配置表中", id)
		}
		if lv > node.maxLevel {
			return nil, 0, fmt.Errorf("专精 %d 等级 %d 超过上限 %d", id, lv, node.maxLevel)
		}
		if node.reqID != 0 && levels[node.reqID] < node.reqLevel {
			return nil, 0, fmt.Errorf("专精 %d 前置未达标", id)
		}
		costs[id] = lv * node.cost
		total += lv * node.cost
	}
	return costs, total, nil
}

// stubOwnership 是拥有权查询的测试替身;owned 为 nil 表示"请求什么就持有什么"。
type stubOwnership struct {
	owned   map[uint64]uint32 // instance_id -> item_config_id
	details map[uint64]data.OwnedEquipmentInstance
	idsOnly bool // 模拟滚动升级中仅回字段 2 的旧 inventory 副本
	err     error
}

func (s stubOwnership) CheckInstancesOwned(
	_ context.Context, _ uint64, equipment []data.EquipmentSlot,
) (data.InstanceOwnershipResult, error) {
	if s.err != nil {
		return data.InstanceOwnershipResult{}, s.err
	}
	result := data.InstanceOwnershipResult{}
	for _, e := range equipment {
		owned := s.owned == nil
		if s.owned != nil {
			owned = s.owned[e.InstanceID] == e.ItemConfigID
		}
		if !owned {
			continue
		}
		result.OwnedInstanceIDs = append(result.OwnedInstanceIDs, e.InstanceID)
		if s.idsOnly {
			continue
		}
		detail, ok := s.details[e.InstanceID]
		if !ok && s.details != nil {
			continue
		}
		if !ok {
			detail = data.OwnedEquipmentInstance{InstanceID: e.InstanceID, ItemConfigID: e.ItemConfigID}
		}
		result.OwnedInstances = append(result.OwnedInstances, detail)
	}
	return result, nil
}

// newUCLoadout 构造开启出战养成的 usecase,并注入三项权威校验依赖的测试替身。
// 不注入的话 SetEquipment / SetTalents 会按设计 fail-closed 拒绝(表未加载)。
func newUCLoadout(repo data.PlayerRepo) *PlayerUsecase {
	uc := NewPlayerUsecase(repo, conf.PlayerConf{BaseMMR: 1500, MMRFloor: 0, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32, HeroSelectionEnabled: true, LoadoutCustomizeEnabled: true})
	uc.itemRules = stubItemRules{slotByItem: map[uint32]uint32{
		1001: 1,
		1002: 2,
	}}
	uc.talentRules = stubTalentRules{nodes: map[uint32]stubTalentNode{
		5001: {maxLevel: 5, cost: 1},
		5002: {maxLevel: 3, cost: 2, reqID: 5001, reqLevel: 2},
	}}
	uc.skillCardRules = stubSkillCardRules{cards: map[uint32]stubSkillCard{
		// 普通卡:上限 3,曲线 5/10。
		7001: {maxLevel: 3, curve: map[uint32]uint32{2: 5, 3: 10}},
		// 传说卡:上限 2,曲线 20。
		7002: {maxLevel: 2, curve: map[uint32]uint32{2: 20}},
		// 曲线断档的卡(上限 3 但只铺到 2 级):用来钉"缺档不得当免费升级放行"。
		7003: {maxLevel: 3, curve: map[uint32]uint32{2: 5}},
	}}
	uc.SetInstanceOwnershipChecker(stubOwnership{})
	return uc
}

type stubSkillCard struct {
	maxLevel uint32
	curve    map[uint32]uint32
}

// stubSkillCardRules 是技能卡配置表判定的测试替身。
type stubSkillCardRules struct {
	cards map[uint32]stubSkillCard
}

func (s stubSkillCardRules) CardExists(cardID uint32) bool {
	_, ok := s.cards[cardID]
	return ok
}

func (s stubSkillCardRules) UpgradeCurve(cardID uint32) (map[uint32]uint32, uint32, error) {
	card, ok := s.cards[cardID]
	if !ok {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "unknown skill card %d", cardID)
	}
	return card.curve, card.maxLevel, nil
}

func TestUpdateMMR_AppliesDelta(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	newMMR, already, err := uc.UpdateMMR(context.Background(), 100, 16, "win", "m1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if already {
		t.Fatal("first call should not be idempotent hit")
	}
	if newMMR != 1516 {
		t.Fatalf("want 1516, got %d", newMMR)
	}
	if repo.players[100].totalBattles != 1 || repo.players[100].totalWins != 1 {
		t.Fatalf("win should inc battle+win, got battles=%d wins=%d",
			repo.players[100].totalBattles, repo.players[100].totalWins)
	}
}

func TestUpdateMMR_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	first, _, err := uc.UpdateMMR(context.Background(), 100, 16, "win", "m1")
	if err != nil {
		t.Fatalf("first err: %v", err)
	}
	second, already, err := uc.UpdateMMR(context.Background(), 100, 16, "win", "m1")
	if err != nil {
		t.Fatalf("second err: %v", err)
	}
	if !already {
		t.Fatal("second call with same key should be idempotent hit")
	}
	if second != first {
		t.Fatalf("idempotent return should equal first: first=%d second=%d", first, second)
	}
	if repo.players[100].mmr != 1516 {
		t.Fatalf("mmr must not double-apply, got %d", repo.players[100].mmr)
	}
	if repo.players[100].totalBattles != 1 {
		t.Fatalf("battles must not double-count, got %d", repo.players[100].totalBattles)
	}
}

func TestUpdateMMR_RequiresKey(t *testing.T) {
	uc := newUC(newFakeRepo())
	_, _, err := uc.UpdateMMR(context.Background(), 100, 16, "win", "")
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("empty idempotency_key should be ErrInvalidArg, got %v", err)
	}
}

func TestUpdateMMR_Floor(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	newMMR, _, err := uc.UpdateMMR(context.Background(), 100, -9999, "lose", "m1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if newMMR != 0 {
		t.Fatalf("mmr should clamp to floor 0, got %d", newMMR)
	}
}

func TestUpdateMMR_LoseCountsBattleNotWin(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, _, err := uc.UpdateMMR(context.Background(), 100, -16, "lose", "m1"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if repo.players[100].totalBattles != 1 || repo.players[100].totalWins != 0 {
		t.Fatalf("lose: battle+1 win+0, got battles=%d wins=%d",
			repo.players[100].totalBattles, repo.players[100].totalWins)
	}
}

func TestUpdateMMR_AbandonNoBattleCount(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, _, err := uc.UpdateMMR(context.Background(), 100, 0, "abandon", "m1"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if repo.players[100].totalBattles != 0 {
		t.Fatalf("abandon should not count battle, got %d", repo.players[100].totalBattles)
	}
}

func TestGetMMR_NotFoundReturnsBase(t *testing.T) {
	uc := newUC(newFakeRepo())
	mmr, err := uc.GetMMR(context.Background(), 999)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if mmr != 1500 {
		t.Fatalf("unbuilt player should return base 1500, got %d", mmr)
	}
}

func TestUnlockHero_Idempotent(t *testing.T) {
	uc := newUC(newFakeRepo())
	if err := uc.UnlockHero(context.Background(), 100, 7, "reward"); err != nil {
		t.Fatalf("first unlock err: %v", err)
	}
	err := uc.UnlockHero(context.Background(), 100, 7, "reward")
	if errcode.As(err) != errcode.ErrPlayerHeroAlreadyOwn {
		t.Fatalf("second unlock should be ErrPlayerHeroAlreadyOwn, got %v", err)
	}
}

func TestUpdateNickname_Validation(t *testing.T) {
	uc := newUC(newFakeRepo())
	if err := uc.UpdateNickname(context.Background(), 100, "   "); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("blank nickname should be ErrInvalidArg, got %v", err)
	}
	if err := uc.UpdateNickname(context.Background(), 0, "ok"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero player_id should be ErrInvalidArg, got %v", err)
	}
}

func TestBattleFlags(t *testing.T) {
	cases := []struct {
		reason              string
		wantBattle, wantWin bool
	}{
		{"win", true, true},
		{"lose", true, false},
		{"draw", true, false},
		{"abandon", false, false},
		{"rollback", false, false},
		{"", false, false},
	}
	for _, c := range cases {
		b, w := battleFlags(c.reason)
		if b != c.wantBattle || w != c.wantWin {
			t.Fatalf("reason=%q: want (battle=%v win=%v), got (%v %v)", c.reason, c.wantBattle, c.wantWin, b, w)
		}
	}
}

// ── 出战养成 ──────────────────────────────────────────────────────────────────

func TestSelectHero_FeatureDisabled(t *testing.T) {
	uc := newUC(newFakeRepo()) // HeroSelectionEnabled=false
	err := uc.SelectHero(context.Background(), 100, 7)
	if errcode.As(err) != errcode.ErrPlayerFeatureDisabled {
		t.Fatalf("disabled toggle should be ErrPlayerFeatureDisabled, got %v", err)
	}
}

func TestSelectHero_NotOwned(t *testing.T) {
	uc := newUCHero(newFakeRepo())
	err := uc.SelectHero(context.Background(), 100, 7)
	if errcode.As(err) != errcode.ErrPlayerHeroLocked {
		t.Fatalf("unowned hero should be ErrPlayerHeroLocked, got %v", err)
	}
}

func TestSelectHero_Success(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCHero(repo)
	if err := uc.UnlockHero(context.Background(), 100, 7, "reward"); err != nil {
		t.Fatalf("unlock err: %v", err)
	}
	if err := uc.SelectHero(context.Background(), 100, 7); err != nil {
		t.Fatalf("select err: %v", err)
	}
	hero, err := uc.GetActiveHero(context.Background(), 100)
	if err != nil {
		t.Fatalf("get active err: %v", err)
	}
	if hero != 7 {
		t.Fatalf("active hero want 7, got %d", hero)
	}
}

func TestGrantAttributePoints_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	first, err := uc.GrantAttributePoints(context.Background(), 100, 5, "lvlup-10")
	if err != nil {
		t.Fatalf("first grant err: %v", err)
	}
	if first != 5 {
		t.Fatalf("first grant unspent want 5, got %d", first)
	}
	second, err := uc.GrantAttributePoints(context.Background(), 100, 5, "lvlup-10")
	if err != nil {
		t.Fatalf("second grant err: %v", err)
	}
	if second != 5 {
		t.Fatalf("idempotent grant should not double-add, want 5, got %d", second)
	}
}

func TestGrantAttributePoints_RequiresKey(t *testing.T) {
	uc := newUC(newFakeRepo())
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 5, ""); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("empty key should be ErrInvalidArg, got %v", err)
	}
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 0, "k"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-positive points should be ErrInvalidArg, got %v", err)
	}
}

func TestAllocateAttributePoints_Insufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 3, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{{Key: "str", Points: 5}})
	if errcode.As(err) != errcode.ErrPlayerInsufficientPoints {
		t.Fatalf("over-allocate should be ErrPlayerInsufficientPoints, got %v", err)
	}
}

func TestAllocateAttributePoints_SuccessThenReset(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	unspent, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{{Key: "str", Points: 3}, {Key: "agi", Points: 2}})
	if err != nil {
		t.Fatalf("allocate err: %v", err)
	}
	if unspent != 5 {
		t.Fatalf("after allocate 5, unspent want 5, got %d", unspent)
	}
	attrs, u2, err := uc.GetAttributes(context.Background(), 100)
	if err != nil {
		t.Fatalf("get attrs err: %v", err)
	}
	if u2 != 5 || len(attrs) != 2 {
		t.Fatalf("want unspent=5 attrs=2, got unspent=%d attrs=%d", u2, len(attrs))
	}
	resetUnspent, err := uc.ResetAttributes(context.Background(), 100)
	if err != nil {
		t.Fatalf("reset err: %v", err)
	}
	if resetUnspent != 10 {
		t.Fatalf("after reset all points return, unspent want 10, got %d", resetUnspent)
	}
}

func TestAllocateAttributePoints_Validation(t *testing.T) {
	uc := newUC(newFakeRepo())
	if _, err := uc.AllocateAttributePoints(context.Background(), 100, nil); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("empty allocs should be ErrInvalidArg, got %v", err)
	}
	if _, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{{Key: "", Points: 1}}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("empty key should be ErrInvalidArg, got %v", err)
	}
	if _, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{{Key: "str", Points: 0}}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-positive points should be ErrInvalidArg, got %v", err)
	}
}

// TestAllocateAttributePoints_Overflow 验证 int32 求和溢出不能反向增加点数(P0-1)。
func TestAllocateAttributePoints_Overflow(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	// 玩家只有 10 点可分配。
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}

	// 两个 MaxInt32 正数:朴素 int32 求和会绕回负数骗过 sum<=unspent,反向增加点数。
	_, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{
		{Key: "str", Points: math.MaxInt32},
		{Key: "agi", Points: math.MaxInt32},
	})
	if err == nil {
		t.Fatalf("summed overflow must be rejected, got nil")
	}
	code := errcode.As(err)
	if code != errcode.ErrPlayerInsufficientPoints && code != errcode.ErrInvalidArg {
		t.Fatalf("overflow want Insufficient/InvalidArg, got %v", err)
	}
	// 零写入:unspent 与已分配属性都不得改变。
	attrs, unspent, gerr := uc.GetAttributes(context.Background(), 100)
	if gerr != nil {
		t.Fatalf("get attrs err: %v", gerr)
	}
	if unspent != 10 {
		t.Fatalf("unspent must stay 10 after rejected overflow, got %d", unspent)
	}
	if len(attrs) != 0 {
		t.Fatalf("no attribute must be written on rejected overflow, got %d", len(attrs))
	}
}

// TestAllocateAttributePoints_DuplicateKeyOverflow 验证重复 attr key 累加不能溢出单列。
func TestAllocateAttributePoints_DuplicateKeyOverflow(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	// 同一 key 两条 MaxInt32:单列累计溢出必须被拒(且零写入)。
	_, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{
		{Key: "str", Points: math.MaxInt32},
		{Key: "str", Points: math.MaxInt32},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("duplicate-key column overflow want ErrInvalidArg, got %v", err)
	}
	attrs, unspent, gerr := uc.GetAttributes(context.Background(), 100)
	if gerr != nil {
		t.Fatalf("get attrs err: %v", gerr)
	}
	if unspent != 10 || len(attrs) != 0 {
		t.Fatalf("rejected overflow must be zero-write, got unspent=%d attrs=%d", unspent, len(attrs))
	}
}

func TestGetLoadout_Snapshot(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCHero(repo)
	if err := uc.UnlockHero(context.Background(), 100, 7, "reward"); err != nil {
		t.Fatalf("unlock err: %v", err)
	}
	if err := uc.SelectHero(context.Background(), 100, 7); err != nil {
		t.Fatalf("select err: %v", err)
	}
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 4, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	if _, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{{Key: "str", Points: 1}}); err != nil {
		t.Fatalf("allocate err: %v", err)
	}
	loadout, err := uc.GetLoadout(context.Background(), 100)
	if err != nil {
		t.Fatalf("loadout err: %v", err)
	}
	if loadout.GetActiveHeroId() != 7 {
		t.Fatalf("loadout hero want 7, got %d", loadout.GetActiveHeroId())
	}
	if loadout.GetUnspentAttrPoints() != 3 {
		t.Fatalf("loadout unspent want 3, got %d", loadout.GetUnspentAttrPoints())
	}
	if len(loadout.GetAttributes()) != 1 {
		t.Fatalf("loadout attrs want 1, got %d", len(loadout.GetAttributes()))
	}
}

// ── 出战装备预设 / 天赋树(W5 ②)──────────────────────────────────────────────

func TestSetEquipment_FeatureDisabled(t *testing.T) {
	uc := newUCHero(newFakeRepo()) // LoadoutCustomizeEnabled=false
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrPlayerFeatureDisabled {
		t.Fatalf("disabled toggle should be ErrPlayerFeatureDisabled, got %v", err)
	}
}

func TestSetEquipment_DuplicateSlot(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9002},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("duplicate slot should be ErrInvalidArg, got %v", err)
	}
}

func TestSetEquipment_RequiresItemConfig(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 0, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero item_config_id should be ErrInvalidArg, got %v", err)
	}
}

func TestSetEquipment_SuccessThenGet(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	if err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
		{Slot: 2, ItemConfigID: 1002, InstanceID: 9002},
	}); err != nil {
		t.Fatalf("set equipment err: %v", err)
	}
	slots, err := uc.GetEquipment(context.Background(), 100)
	if err != nil {
		t.Fatalf("get equipment err: %v", err)
	}
	if len(slots) != 2 {
		t.Fatalf("equipment want 2 slots, got %d", len(slots))
	}
}

func TestGrantTalentPoints_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	first, err := uc.GrantTalentPoints(context.Background(), 100, 6, "lvlup-20")
	if err != nil {
		t.Fatalf("first grant err: %v", err)
	}
	if first != 6 {
		t.Fatalf("first talent grant unspent want 6, got %d", first)
	}
	second, err := uc.GrantTalentPoints(context.Background(), 100, 6, "lvlup-20")
	if err != nil {
		t.Fatalf("second grant err: %v", err)
	}
	if second != 6 {
		t.Fatalf("idempotent talent grant should not double-add, want 6, got %d", second)
	}
}

func TestSetTalents_Insufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 2, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 5001, Level: 3}})
	if errcode.As(err) != errcode.ErrPlayerInsufficientPoints {
		t.Fatalf("over-spec should be ErrPlayerInsufficientPoints, got %v", err)
	}
}

func TestSetTalents_DuplicateTalent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 5, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 5001, Level: 1}, {TalentID: 5001, Level: 1}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("duplicate talent_id should be ErrInvalidArg, got %v", err)
	}
}

func TestSetTalents_SuccessThenResetAndLoadout(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	if err := uc.UnlockHero(context.Background(), 100, 7, "reward"); err != nil {
		t.Fatalf("unlock err: %v", err)
	}
	if err := uc.SelectHero(context.Background(), 100, 7); err != nil {
		t.Fatalf("select err: %v", err)
	}
	if err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}); err != nil {
		t.Fatalf("set equipment err: %v", err)
	}
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 5, "g1"); err != nil {
		t.Fatalf("grant talent err: %v", err)
	}
	unspent, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 5001, Level: 2}})
	if err != nil {
		t.Fatalf("set talents err: %v", err)
	}
	if unspent != 3 {
		t.Fatalf("after spec 2 of 5, talent unspent want 3, got %d", unspent)
	}

	loadout, err := uc.GetLoadout(context.Background(), 100)
	if err != nil {
		t.Fatalf("loadout err: %v", err)
	}
	if len(loadout.GetEquipment()) != 1 {
		t.Fatalf("loadout equipment want 1, got %d", len(loadout.GetEquipment()))
	}
	if len(loadout.GetTalents()) != 1 {
		t.Fatalf("loadout talents want 1, got %d", len(loadout.GetTalents()))
	}
	if loadout.GetUnspentTalentPoints() != 3 {
		t.Fatalf("loadout talent unspent want 3, got %d", loadout.GetUnspentTalentPoints())
	}

	resetUnspent, err := uc.ResetTalents(context.Background(), 100)
	if err != nil {
		t.Fatalf("reset talents err: %v", err)
	}
	if resetUnspent != 5 {
		t.Fatalf("after reset talents all return, want 5, got %d", resetUnspent)
	}
}
