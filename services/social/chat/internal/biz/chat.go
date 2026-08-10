// Package biz 是 chat 服务的业务逻辑层(2026-06-16)。
//
// 职责(docs/design/go-services.md §2.5):
//   - 五频道聊天:世界(WORLD)/ 队伍(TEAM)/ 私聊(PRIVATE)/ 公会(GUILD)/ 临时群(GROUP)
//   - 服务端校验:频道合法性 + 内容长度(utf8 rune ≤ MaxContentLen)+ 敏感词屏蔽
//   - 私聊落 pandora_social(MySQL,支持离线 PullHistory);公会 / 群是即时频道,不落聊天历史
//   - 五频道经 kafka pandora.chat.{world,team,private,guild,group} → push 推送(弱依赖)
//   - 队伍 / 公会 / 群成员经对应服务 gRPC 解析(弱依赖)
//
// 关键规则:
//   - 客户端不能发 SYSTEM / UNSPECIFIED 频道 → ErrChatChannelInvalid
//   - 推送原则 2:队伍 / 私聊 / 公会 / 群只发收件方,不回发自己(客户端本地回显己方消息)
//   - 世界频道是广播:to_player_id=0,key 空,由 push 服务 Broadcast(原则 2 例外)
//   - 公会 / 群聊历史不落库:即时频道,离线消息不补发(用户确认 2026-06-27)
//   - sender_nickname 留空:由客户端按 sender_id 解析展示名(CLAUDE.md §5.8 最小数据单位)
package biz

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"

	"github.com/luyuancpp/pandora/services/social/chat/internal/conf"
	"github.com/luyuancpp/pandora/services/social/chat/internal/data"
)

// ChatPusher 把聊天推送事件发到 kafka(main.go 注入 kafkax 适配器;弱依赖,nil 时静默跳过)。
// 五个方法对应五个 topic;key 由适配器按收件方 player_id 设置(世界频道 key 空)。
type ChatPusher interface {
	PushPrivate(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error
	PushTeam(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error
	PushWorld(ctx context.Context, evt *chatv1.ChatPushEvent) error
	PushGuild(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error
	PushGroup(ctx context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error
}

// TeamReader 解析队伍成员名单(main.go 注入 team gRPC 适配器;弱依赖,nil 时 TEAM 降级)。
type TeamReader interface {
	GetTeamMembers(ctx context.Context, teamID uint64) ([]uint64, bool, error)
}

// GuildReader 解析公会成员名单(main.go 注入 guild gRPC 适配器;弱依赖,nil 时 GUILD 降级)。
type GuildReader interface {
	GetGuildMembers(ctx context.Context, guildID uint64) ([]uint64, bool, error)
}

// GroupReader 解析临时群成员名单(main.go 注入 group gRPC 适配器;弱依赖,nil 时 GROUP 降级)。
type GroupReader interface {
	GetGroupMembers(ctx context.Context, groupID uint64) ([]uint64, bool, error)
}

// WorldRateLimiter 世界频道发送冷却判定(压测审核【必修-5】)。
//
// 实现须跨副本一致(data.RedisWorldRateLimiter:SET NX PX 原子占窗),因为 chat 可水平
// 扩展,单进程内存限流会被多副本摊薄。返回 allowed=false 表示冷却期内(调用方拒绝);
// 判定失败(Redis 抖动)返回 error,由调用方 fail-open 放行——限流是背压手段而非权威门,
// 依赖故障时牺牲限流保聊天可用(§9.22 fail-closed 只约束权威写决策)。
type WorldRateLimiter interface {
	AllowWorld(ctx context.Context, playerID uint64, cooldown time.Duration) (bool, error)
}

// ChannelRateLimiter 非世界频道(private/team/guild/group)的 per-player per-频道冷却
// (anti-abuse §6 第 6 项)。语义同 WorldRateLimiter:背压非权威门,判定 error 由调用方
// fail-open 放行;实现 data.RedisWorldRateLimiter 同一结构体同时提供两个接口。
type ChannelRateLimiter interface {
	AllowChannel(ctx context.Context, channel string, playerID uint64, cooldown time.Duration) (bool, error)
}

// ChatUsecase 是 chat 服务业务逻辑核心。
type ChatUsecase struct {
	repo   data.PrivateRepo
	pusher ChatPusher  // 弱依赖,可为 nil
	team   TeamReader  // 弱依赖,可为 nil
	guild  GuildReader // 弱依赖,可为 nil
	group  GroupReader // 弱依赖,可为 nil
	cfg    conf.ChatConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分区,私聊跨 region 投递落点观测退化为不打日志(行为不变)。
	// 区域总线部署时由 main 经 SetCellRouter 注入,sendPrivate 落库后额外打一条私聊跨 region
	// 投递落点观测(跨 region 走全局桥,同 region 走区域总线)。nil-safe。
	router *cellroute.Router

	// worldLimiter 世界频道 per-player 冷却(压测审核【必修-5】)。可为 nil:
	// 未配 Redis 的骨架联调不限流,行为与历史一致;生产 main 必接线。
	worldLimiter WorldRateLimiter

	// channelLimiter 非世界频道冷却(anti-abuse §6 第 6 项)。可为 nil(不限流)。
	channelLimiter ChannelRateLimiter
}

// NewChatUsecase 构造。pusher / team / guild / group 允许为 nil(弱依赖未配置时降级)。
func NewChatUsecase(repo data.PrivateRepo, pusher ChatPusher, team TeamReader, guild GuildReader, group GroupReader, cfg conf.ChatConf) *ChatUsecase {
	if cfg.MaxContentLen <= 0 {
		cfg.MaxContentLen = 256
	}
	if cfg.HistoryLimit <= 0 {
		cfg.HistoryLimit = 50
	}
	return &ChatUsecase{repo: repo, pusher: pusher, team: team, guild: guild, group: group, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),sendPrivate 不做私聊跨 region 投递落点
// 观测,行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名
// (与 matchmaker / auction / battle_result / friend 一致)。Router 内部读路径无锁,并发安全。
func (u *ChatUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetWorldRateLimiter 注入世界频道冷却判定(main 按 node.redis_client 装配)。
// nil-safe:不调用时世界频道不限流(骨架联调),与历史行为一致。
func (u *ChatUsecase) SetWorldRateLimiter(l WorldRateLimiter) {
	u.worldLimiter = l
}

// SetChannelRateLimiter 注入非世界频道冷却判定(main 与 world limiter 同一实例装配)。
// nil-safe:不调用时非世界频道不限流,与历史行为一致。
func (u *ChatUsecase) SetChannelRateLimiter(l ChannelRateLimiter) {
	u.channelLimiter = l
}

// allowChannel 非世界频道冷却门:窗内拒绝返回 ErrRateLimited(零副作用,先于落库/推送);
// limiter 未注入 / 冷却 <=0 / 判定失败(Redis 抖动)一律放行(fail-open,§2 铁律)。
func (u *ChatUsecase) allowChannel(ctx context.Context, channel string, senderID uint64) error {
	if u.channelLimiter == nil {
		return nil
	}
	cooldown := u.cfg.NonWorldCooldown.Std()
	if cooldown <= 0 {
		return nil
	}
	allowed, err := u.channelLimiter.AllowChannel(ctx, channel, senderID, cooldown)
	if err != nil {
		plog.With(ctx).Warnw("msg", "chat_channel_ratelimit_check_failed",
			"channel", channel, "sender_id", senderID, "err", err)
		return nil
	}
	if !allowed {
		return errcode.New(errcode.ErrRateLimited, "%s chat cooldown, retry after %s", channel, cooldown)
	}
	return nil
}

// SendMessage 发一条聊天消息。senderID 由 service 从 JWT ctx 得到(R5)。
// newMessageID 是 service 用 snowflake 预生成的消息 ID。
func (u *ChatUsecase) SendMessage(
	ctx context.Context,
	senderID uint64,
	channel chatv1.ChatChannel,
	targetID uint64,
	content string,
	newMessageID uint64,
) (uint64, error) {
	if senderID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "sender required")
	}

	// 频道校验:客户端只能发 WORLD / TEAM / PRIVATE / GUILD / GROUP。
	switch channel {
	case chatv1.ChatChannel_CHAT_CHANNEL_WORLD,
		chatv1.ChatChannel_CHAT_CHANNEL_TEAM,
		chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE,
		chatv1.ChatChannel_CHAT_CHANNEL_GUILD,
		chatv1.ChatChannel_CHAT_CHANNEL_GROUP:
	default:
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "channel %d not allowed from client", channel)
	}

	// 内容校验:非空 + utf8 rune 长度 ≤ MaxContentLen。
	content = strings.TrimSpace(content)
	if content == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "empty content")
	}
	if utf8.RuneCountInString(content) > u.cfg.MaxContentLen {
		return 0, errcode.New(errcode.ErrChatMessageTooLong,
			"content too long: %d > %d", utf8.RuneCountInString(content), u.cfg.MaxContentLen)
	}
	content = u.maskSensitive(content)

	msg := &chatv1.ChatMessage{
		MessageId:  newMessageID,
		SenderId:   senderID,
		Channel:    channel,
		TargetId:   targetID,
		Content:    content,
		SendTimeMs: nowMs(),
		// SenderNickname 留空,客户端按 sender_id 解析(最小数据单位)。
	}

	// 非世界频道冷却(anti-abuse §6 第 6 项):统一在分发点、一切副作用之前判定,
	// 按频道独立占窗(队聊不占私聊的窗);世界频道另有更严的 sendWorld 冷却。
	switch channel {
	case chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE:
		if err := u.allowChannel(ctx, "private", senderID); err != nil {
			return 0, err
		}
		return u.sendPrivate(ctx, msg)
	case chatv1.ChatChannel_CHAT_CHANNEL_TEAM:
		if err := u.allowChannel(ctx, "team", senderID); err != nil {
			return 0, err
		}
		return u.sendTeam(ctx, senderID, msg)
	case chatv1.ChatChannel_CHAT_CHANNEL_GUILD:
		if err := u.allowChannel(ctx, "guild", senderID); err != nil {
			return 0, err
		}
		return u.sendGuild(ctx, senderID, msg)
	case chatv1.ChatChannel_CHAT_CHANNEL_GROUP:
		if err := u.allowChannel(ctx, "group", senderID); err != nil {
			return 0, err
		}
		return u.sendGroup(ctx, senderID, msg)
	default: // WORLD
		return u.sendWorld(ctx, msg)
	}
}

// sendPrivate 私聊:必须有 target,落库(离线历史)+ 推送给接收方(原则 2)。
func (u *ChatUsecase) sendPrivate(ctx context.Context, msg *chatv1.ChatMessage) (uint64, error) {
	if msg.GetTargetId() == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "private chat requires target_id")
	}
	if msg.GetTargetId() == msg.GetSenderId() {
		return 0, errcode.New(errcode.ErrInvalidArg, "cannot private chat self")
	}

	// 落库强依赖:私聊历史不可丢(MySQL 失败则整条失败,让客户端重试)。
	if err := u.repo.SavePrivate(ctx, msg); err != nil {
		return 0, err
	}

	// 推送弱依赖:发给接收方;失败只 warn(消息已落库,接收方上线 PullHistory 兜底)。
	if u.pusher != nil {
		evt := &chatv1.ChatPushEvent{Message: msg, ToPlayerId: msg.GetTargetId()}
		if err := u.pusher.PushPrivate(ctx, msg.GetTargetId(), evt); err != nil {
			plog.With(ctx).Warnw("msg", "chat_private_push_failed",
				"to_player_id", msg.GetTargetId(), "message_id", msg.GetMessageId(), "err", err)
		}
	}

	// 多 region:观测本条私聊的跨 region 投递落点(跨 region → 走全局桥,同 region → 走区域总线)。
	// router 为 nil(单 Cell)→ 不打,行为不变;跨 region Kafka 桥 / 区域总线拆分属 infra(§11.1)。
	u.logPrivateRouting(ctx, msg.GetSenderId(), msg.GetTargetId())
	return msg.GetMessageId(), nil
}

// sendTeam 队伍频道:target_id 即 team_id;解析成员逐个推送(排除发送者,原则 2)。
// team / pusher 弱依赖,缺失时静默降级(消息不持久化,队伍频道是即时频道)。
func (u *ChatUsecase) sendTeam(ctx context.Context, senderID uint64, msg *chatv1.ChatMessage) (uint64, error) {
	teamID := msg.GetTargetId()
	if teamID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "team chat requires target_id (team_id)")
	}
	if u.team == nil || u.pusher == nil {
		// 弱依赖未配置:不报错,返回 message_id(客户端本地回显),仅记一条 warn。
		plog.With(ctx).Warnw("msg", "chat_team_degraded", "team_id", teamID,
			"hint", "team reader / pusher not configured, team chat fan-out skipped")
		return msg.GetMessageId(), nil
	}

	members, ok, err := u.team.GetTeamMembers(ctx, teamID)
	if err != nil {
		// team 服务暂时不可达:诚实报错让客户端重试。不能假成功——成员无法解析则
		// 没有任何人收到消息,返回 message_id 会让发送者以为已送达(消息静默丢失),
		// 且成员身份校验被跳过。
		plog.With(ctx).Warnw("msg", "chat_team_resolve_failed", "team_id", teamID, "err", err)
		return 0, errcode.New(errcode.ErrUnavailable, "team %d members unavailable, retry later", teamID)
	}
	if !ok {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "team %d not found", teamID)
	}

	// 发送者必须是队伍成员才能在队伍频道说话。
	inTeam := false
	for _, m := range members {
		if m == senderID {
			inTeam = true
			break
		}
	}
	if !inTeam {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "sender %d not in team %d", senderID, teamID)
	}

	// 模式 C:kafka 弱依赖失败时逐成员打 Warn 会按成员数刷屏(消息本身已落库、RPC 仍返回
	// 成功,access log 记 rpc_ok,所以这条是"接收方漏推"的唯一信号,不能删)。改为循环内
	// 累加、循环后一条:既保留信号,又给出"N 个成员里漏了几个"。
	var failed int
	var firstErr error
	var sampleTo uint64
	for _, m := range members {
		if m == senderID {
			continue // 原则 2:不回发自己
		}
		evt := &chatv1.ChatPushEvent{Message: msg, ToPlayerId: m}
		if perr := u.pusher.PushTeam(ctx, m, evt); perr != nil {
			failed++
			if firstErr == nil {
				firstErr, sampleTo = perr, m
			}
		}
	}
	if failed > 0 {
		plog.With(ctx).Warnw("msg", "chat_team_push_failed",
			"team_id", teamID, "members", len(members), "failed", failed,
			"sample_to_player_id", sampleTo, "first_err", firstErr)
	}
	return msg.GetMessageId(), nil
}

// sendGuild 公会频道:target_id 即 guild_id;解析成员逐个推送(排除发送者,原则 2)。
// guild / pusher 弱依赖,缺失时静默降级(消息不持久化,公会频道是即时频道,历史不落库)。
func (u *ChatUsecase) sendGuild(ctx context.Context, senderID uint64, msg *chatv1.ChatMessage) (uint64, error) {
	guildID := msg.GetTargetId()
	if guildID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "guild chat requires target_id (guild_id)")
	}
	if u.guild == nil || u.pusher == nil {
		// 弱依赖未配置:不报错,返回 message_id(客户端本地回显),仅记一条 warn。
		plog.With(ctx).Warnw("msg", "chat_guild_degraded", "guild_id", guildID,
			"hint", "guild reader / pusher not configured, guild chat fan-out skipped")
		return msg.GetMessageId(), nil
	}

	members, ok, err := u.guild.GetGuildMembers(ctx, guildID)
	if err != nil {
		// guild 服务暂时不可达:诚实报错让客户端重试(同 sendTeam:假成功 = 消息静默丢失 + 跳过成员校验)。
		plog.With(ctx).Warnw("msg", "chat_guild_resolve_failed", "guild_id", guildID, "err", err)
		return 0, errcode.New(errcode.ErrUnavailable, "guild %d members unavailable, retry later", guildID)
	}
	if !ok {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "guild %d not found", guildID)
	}

	// 发送者必须是公会成员才能在公会频道说话。
	inGuild := false
	for _, m := range members {
		if m == senderID {
			inGuild = true
			break
		}
	}
	if !inGuild {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "sender %d not in guild %d", senderID, guildID)
	}

	// 模式 C:公会成员上限 100(§9.18),kafka 一挂单条公会消息就能刷 100 行 → 批末汇总一条。
	var failed int
	var firstErr error
	var sampleTo uint64
	for _, m := range members {
		if m == senderID {
			continue // 原则 2:不回发自己
		}
		evt := &chatv1.ChatPushEvent{Message: msg, ToPlayerId: m}
		if perr := u.pusher.PushGuild(ctx, m, evt); perr != nil {
			failed++
			if firstErr == nil {
				firstErr, sampleTo = perr, m
			}
		}
	}
	if failed > 0 {
		plog.With(ctx).Warnw("msg", "chat_guild_push_failed",
			"guild_id", guildID, "members", len(members), "failed", failed,
			"sample_to_player_id", sampleTo, "first_err", firstErr)
	}
	return msg.GetMessageId(), nil
}

// sendGroup 临时群频道:target_id 即 group_id;解析成员逐个推送(排除发送者,原则 2)。
// group / pusher 弱依赖,缺失时静默降级(消息不持久化,群频道是即时频道,历史不落库)。
func (u *ChatUsecase) sendGroup(ctx context.Context, senderID uint64, msg *chatv1.ChatMessage) (uint64, error) {
	groupID := msg.GetTargetId()
	if groupID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "group chat requires target_id (group_id)")
	}
	if u.group == nil || u.pusher == nil {
		// 弱依赖未配置:不报错,返回 message_id(客户端本地回显),仅记一条 warn。
		plog.With(ctx).Warnw("msg", "chat_group_degraded", "group_id", groupID,
			"hint", "group reader / pusher not configured, group chat fan-out skipped")
		return msg.GetMessageId(), nil
	}

	members, ok, err := u.group.GetGroupMembers(ctx, groupID)
	if err != nil {
		// group 服务暂时不可达:诚实报错让客户端重试(同 sendTeam:假成功 = 消息静默丢失 + 跳过成员校验)。
		plog.With(ctx).Warnw("msg", "chat_group_resolve_failed", "group_id", groupID, "err", err)
		return 0, errcode.New(errcode.ErrUnavailable, "group %d members unavailable, retry later", groupID)
	}
	if !ok {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "group %d not found", groupID)
	}

	// 发送者必须是群成员才能在群频道说话。
	inGroup := false
	for _, m := range members {
		if m == senderID {
			inGroup = true
			break
		}
	}
	if !inGroup {
		return 0, errcode.New(errcode.ErrChatChannelInvalid, "sender %d not in group %d", senderID, groupID)
	}

	// 模式 C:临时群成员上限 50(§9.18),kafka 一挂单条群消息刷 50 行 → 批末汇总一条。
	var failed int
	var firstErr error
	var sampleTo uint64
	for _, m := range members {
		if m == senderID {
			continue // 原则 2:不回发自己
		}
		evt := &chatv1.ChatPushEvent{Message: msg, ToPlayerId: m}
		if perr := u.pusher.PushGroup(ctx, m, evt); perr != nil {
			failed++
			if firstErr == nil {
				firstErr, sampleTo = perr, m
			}
		}
	}
	if failed > 0 {
		plog.With(ctx).Warnw("msg", "chat_group_push_failed",
			"group_id", groupID, "members", len(members), "failed", failed,
			"sample_to_player_id", sampleTo, "first_err", firstErr)
	}
	return msg.GetMessageId(), nil
}

// sendWorld 世界频道:广播(to_player_id=0,key 空,push 服务 Broadcast,原则 2 例外)。
//
// 冷却前置(压测审核【必修-5】):广播成本 ≈ 发送速率 × 全服在线数,必须在生产侧压掉
// 刷屏;冷却期内直接 ErrRateLimited,不产生任何 kafka 写。限流判定失败 fail-open
// (见 WorldRateLimiter 契约):牺牲限流保聊天可用,Warn 留证。
func (u *ChatUsecase) sendWorld(ctx context.Context, msg *chatv1.ChatMessage) (uint64, error) {
	if u.worldLimiter != nil {
		cooldown := u.cfg.WorldCooldown.Std()
		if cooldown > 0 {
			allowed, lerr := u.worldLimiter.AllowWorld(ctx, msg.GetSenderId(), cooldown)
			if lerr != nil {
				plog.With(ctx).Warnw("msg", "chat_world_ratelimit_check_failed",
					"sender_id", msg.GetSenderId(), "err", lerr)
			} else if !allowed {
				return 0, errcode.New(errcode.ErrRateLimited,
					"world chat cooldown, retry after %s", cooldown)
			}
		}
	}
	if u.pusher == nil {
		plog.With(ctx).Warnw("msg", "chat_world_degraded", "hint", "pusher not configured")
		return msg.GetMessageId(), nil
	}
	evt := &chatv1.ChatPushEvent{Message: msg, ToPlayerId: 0}
	if err := u.pusher.PushWorld(ctx, evt); err != nil {
		plog.With(ctx).Warnw("msg", "chat_world_push_failed", "message_id", msg.GetMessageId(), "err", err)
	}
	return msg.GetMessageId(), nil
}

// PullHistory 拉私聊历史。只有 PRIVATE 频道有持久化历史;其余频道返回空。
// player_id 由 service 从 JWT ctx 得到(R5)。
func (u *ChatUsecase) PullHistory(
	ctx context.Context,
	playerID uint64,
	channel chatv1.ChatChannel,
	peerID uint64,
	limit int,
	beforeMs int64,
) ([]*chatv1.ChatMessage, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if channel != chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE {
		// 世界 / 队伍是即时频道,不持久化,无历史可拉。
		return nil, nil
	}
	if peerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "peer_id required for private history")
	}
	if limit <= 0 || limit > u.cfg.HistoryLimit {
		limit = u.cfg.HistoryLimit
	}
	return u.repo.ListPrivate(ctx, playerID, peerID, limit, beforeMs)
}

// maskSensitive 把命中的敏感词整词替换为等长 *。
// 列表为空时直接返回原文(默认不过滤);仅做最小化屏蔽,真正风控由独立服务接管(后续)。
func (u *ChatUsecase) maskSensitive(content string) string {
	if len(u.cfg.SensitiveWords) == 0 {
		return content
	}
	out := content
	for _, w := range u.cfg.SensitiveWords {
		if w == "" {
			continue
		}
		out = strings.ReplaceAll(out, w, strings.Repeat("*", utf8.RuneCountInString(w)))
	}
	return out
}

// nowMs 返回当前毫秒时间戳。
func nowMs() int64 {
	return time.Now().UnixMilli()
}
