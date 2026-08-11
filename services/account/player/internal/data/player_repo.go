// Package data 是 player 服务的数据层(MySQL 玩家档案 / 段位 / 英雄池)。
//
// 库表(deploy/mysql-init/04-player-tables.sql):
//
//	pandora_player.players        玩家档案(PK player_id,uk nickname)
//	pandora_player.player_heroes  英雄解锁(uk player_id+hero_id)
//	pandora_player.mmr_history    MMR 变化历史 + 幂等键(uk player_id+idempotency_key)
//
// 幂等:ApplyMMRChange 在一个事务里 INSERT mmr_history;命中 1062 唯一键冲突 → 视为
// 已处理(already=true),读回该幂等键已记录的 new_mmr 返回,不重复改 players.mmr。
// players 表是结构化列(docs CLAUDE.md §5.9 不强制 proto 化),直接映射 proto 字段。
package data

import (
	"context"
	"database/sql"
	"strings"
	"time"

	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// MMRChange 是一次 MMR 变更请求(biz 算好语义后传给 data 落库)。
type MMRChange struct {
	PlayerID       uint64
	IdempotencyKey string // 一般是 match_id 字符串
	Delta          int32
	Reason         string
	Floor          int  // MMR 下限(clamp 用)
	IncBattle      bool // 是否计一场对局(total_battles+1)
	IncWin         bool // 是否计一胜(total_wins+1)
}

// AttrAllocation 是一次加点请求里对某属性增加的点数(只增,Points>0)。
type AttrAllocation struct {
	Key    string
	Points int32
}

// AttrPoint 是某条属性的已分配点数。
type AttrPoint struct {
	Key    string
	Points int32
}

// EquipmentSlot 是出战装备预设的一个槽位。
type EquipmentSlot struct {
	Slot         uint32
	ItemConfigID uint32
}

// TalentLevel 是天赋树某节点的已点等级与该节点实际消耗的天赋点。
//
// SpentPoints = 等级 × 专精表 cost_per_level,写入时由 biz 按表算好填入、随分配一起落库;
// 读取时由 repo 回填。之所以要存而不是读时再算:repo 层看不到配置表,只能按 Σ 等级 反推,
// 而 cost_per_level≠1 时反推会把已花点数算少,玩家看到的可点数比实际多(写扣 6 读算 4)。
// 存下来后写与读共用专精表这一个口径,且策划改表不会追溯改写老玩家已花掉的点。
type TalentLevel struct {
	TalentID uint32
	Level    int32
	// SpentPoints 该节点实际消耗的天赋点;GetTalents 回填,SetTalents 由 biz 填。
	SpentPoints int32
}

// PlayerRepo 是 player 数据层抽象。biz 层只依赖此接口,不依赖 *sql.DB。
type PlayerRepo interface {
	// EnsureProfile 确保玩家档案存在(INSERT IGNORE 默认档案),已存在则不动。
	EnsureProfile(ctx context.Context, playerID uint64, defaultNickname string, baseMMR int) error
	// GetProfile 读玩家档案。not found → (nil, false, nil)。
	GetProfile(ctx context.Context, playerID uint64) (*playerv1.PlayerProfile, bool, error)
	// UpdateNickname 改昵称。昵称被占用 → ErrPlayerNicknameTaken;玩家不存在 → ErrPlayerNotFound。
	UpdateNickname(ctx context.Context, playerID uint64, nickname string) error
	// ListHeroes 列出玩家已解锁英雄(配置表 hero_id,uint32)。
	ListHeroes(ctx context.Context, playerID uint64) ([]uint32, error)
	// UnlockHero 解锁英雄。已拥有 → (true, nil) 幂等命中。
	UnlockHero(ctx context.Context, playerID uint64, heroID uint32, source string) (already bool, err error)
	// GetMMR 读玩家当前 MMR。not found → (0, false, nil)。
	GetMMR(ctx context.Context, playerID uint64) (mmr int, found bool, err error)
	// ApplyMMRChange 幂等改 MMR + 战绩计数。命中幂等键 → (已记录 new_mmr, true, nil)。
	ApplyMMRChange(ctx context.Context, change MMRChange) (newMMR int, already bool, err error)

	// ── 出战养成 ──────────────────────────────────────────────────────────
	// IsHeroOwned 判断玩家是否已解锁该英雄。
	IsHeroOwned(ctx context.Context, playerID uint64, heroID uint32) (bool, error)
	// SetActiveHero 设定出战英雄(玩家须先 EnsureProfile;英雄拥有校验在 biz 层)。
	SetActiveHero(ctx context.Context, playerID uint64, heroID uint32) error
	// GetActiveHero 读出战英雄。未选定 / 未建档 → (0, nil)。
	GetActiveHero(ctx context.Context, playerID uint64) (uint32, error)
	// GrantAttributePoints 幂等授予可分配点。命中幂等键 → (当前 unspent, true, nil)。
	GrantAttributePoints(ctx context.Context, playerID uint64, points int32, idempotencyKey string) (unspent int, already bool, err error)
	// AllocateAttributePoints 分配点(事务:校验 unspent>=sum,扣减,累加 player_attributes)。
	// 点数不足 → ErrPlayerInsufficientPoints。
	AllocateAttributePoints(ctx context.Context, playerID uint64, allocs []AttrAllocation) (unspent int, err error)
	// ResetAttributes 洗点(已分配点全退回 unspent,清空 player_attributes)。
	ResetAttributes(ctx context.Context, playerID uint64) (unspent int, err error)
	// GetAttributes 读已分配属性点 + 未分配点。
	GetAttributes(ctx context.Context, playerID uint64) (attrs []AttrPoint, unspent int, err error)

	// ── 出战装备预设 / 天赋树 ────────────────────────────────────
	// SetEquipment 全量替换出战装备预设(事务:删旧 + 插新)。
	SetEquipment(ctx context.Context, playerID uint64, slots []EquipmentSlot) error
	// GetEquipment 读出战装备预设(按 slot 排序)。
	GetEquipment(ctx context.Context, playerID uint64) ([]EquipmentSlot, error)
	// GrantTalentPoints 幂等授予天赋点(total_talent_points += points)。命中幂等键 → (当前可点, true, nil)。
	GrantTalentPoints(ctx context.Context, playerID uint64, points int32, idempotencyKey string) (unspent int, already bool, err error)
	// SetTalents 全量重置天赋(事务:校验总消耗<=total,替换 player_talents)。点数不足 → ErrPlayerInsufficientPoints。
	//
	// 每条 TalentLevel.SpentPoints 由 biz 按专精表算好后传入(等级 × cost_per_level),
	// repo 不自己按 sum(level) 推算:每级消耗是配置表列,repo 层看不到配置,
	// 自行推算会在 cost_per_level≠1 时算少扣。总消耗 = Σ SpentPoints,repo 内直接累加,
	// 不再另收一个总数参数——两个来源就有漂移的可能。
	SetTalents(ctx context.Context, playerID uint64, talents []TalentLevel) (unspent int, err error)
	// ResetTalents 清空天赋(返回 total = 全部可点)。
	ResetTalents(ctx context.Context, playerID uint64) (unspent int, err error)
	// GetTalents 读已点天赋 + 可点天赋点(total - SUM(spent_points))。
	GetTalents(ctx context.Context, playerID uint64) (talents []TalentLevel, unspent int, err error)

	// ── 玩家等级经验(实时成长)────────────────────────────────────────────
	// ApplyExperience 幂等入账经验 + 等级结算 + 经验推送出箱(同一事务)。
	// 命中幂等键 → (当前权威快照, true, nil);满级 → no-op 返回满级快照。
	ApplyExperience(ctx context.Context, apply ExpApply) (ExpState, bool, error)
	// FetchPushOutbox 按 id 升序取最多 limit 条待发布玩家推送出箱记录(FIFO 保序)。
	FetchPushOutbox(ctx context.Context, limit int) ([]PushOutboxRecord, error)
	// DeletePushOutbox 删除已成功投递的推送出箱行。
	DeletePushOutbox(ctx context.Context, id int64) error
	// PurgeExpHistory 删除 created_at < cutoff 的经验幂等收据(最多 limit 行,返回删除数)。
	PurgeExpHistory(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	// PurgeMMRHistory / PurgeAttrPointGrants / PurgeTalentPointGrants 删除 created_at < cutoff
	// 的幂等历史行(最多 limit 行,返回删除数;CLAUDE.md §9 不变量 24)。
	// 前置条件同 exp_history:上游重放(kafka player.update 消费 / 授予补扫)须有小于
	// 保留期的有界期限,否则清掉幂等行 = 同一事件双发(重复加段位分/加点)。
	PurgeMMRHistory(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	PurgeAttrPointGrants(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	PurgeTalentPointGrants(ctx context.Context, cutoff time.Time, limit int) (int64, error)

	// ── 领奖记录 ───────────────────────────────────────────────────────────
	// LoadRewardClaims 读玩家领奖记录(RewardClaimStorageRecord 序列化 bytes + 乐观锁版本)。
	// 未建行 → (nil, 0, nil)(版本 0 表示后续写入按新建处理)。
	LoadRewardClaims(ctx context.Context, playerID uint64) (record []byte, version int32, err error)
	// SaveRewardClaims 乐观锁写领奖记录:
	//   expectVersion == 0 → INSERT(冲突 → ErrPlayerVersionMismatch);
	//   expectVersion  > 0 → UPDATE ... WHERE version=expectVersion(0 行 → ErrPlayerVersionMismatch)。
	SaveRewardClaims(ctx context.Context, playerID uint64, record []byte, expectVersion int32) error
}

// MySQLPlayerRepo 是基于 database/sql 的 PlayerRepo 实现。
type MySQLPlayerRepo struct {
	db *sql.DB
}

// NewMySQLPlayerRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_player 库)。
func NewMySQLPlayerRepo(db *sql.DB) *MySQLPlayerRepo {
	return &MySQLPlayerRepo{db: db}
}

// isDupErr 判断是否 MySQL 1062 唯一键冲突(go-sql-driver 错误串含 "Error 1062")。
func isDupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}
