// Package kafkax — push 推送 topic 常量(W3 ④,2026-06-05)。
//
// 集中在一处定义 6 个推送 topic 名,push 服务消费侧 + 未来业务服 producer 共享,
// 防止字符串拼写漂移。每个 topic 对应一个 proto Event message(payload bytes 反序列化)。
//
// 不变量(docs/design/protocol-ordering-rules.md):
//   - 玩家相关 topic 的 kafka key = strconv.FormatUint(player_id, 10),保证同玩家事件保序
//   - system.notify / chat.world 是广播类,key 可空(由 push 服务 Broadcast 处理)
//   - 业务 producer 必须用 PushToPlayers helper 排除 caller_player_id(原则 2)
//
// W3 ④ 仅订阅 proto 已就绪的 3 个(team.update / match.progress / chat.private);
// 其余 3 个(player.update / friend.event / system.notify)等对应业务服上线时补
// Event message + 把 topic 名加进 etc/push-dev.yaml。
package kafkax

import "github.com/luyuancpp/pandora/pkg/config"

// Push topic 名常量。
//
// 命名规则:`pandora.<domain>.<event_kind>`(小写 + 点分,跟 mysql/redis key 一致)。
const (
	// TopicTeamUpdate — proto: pandora.team.v1.TeamUpdateEvent
	// key=player_id;原则 2:不发给发起方
	TopicTeamUpdate = "pandora.team.update"

	// TopicMatchProgress — proto: pandora.match.v1.MatchProgressEvent
	// key=player_id;**原则 3 例外**:stage 异步变化必须发给所有人(含发起方)
	TopicMatchProgress = "pandora.match.progress"

	// TopicChatWorld — proto: pandora.chat.v1.ChatPushEvent
	// 全服广播(key 暂留空,push 服务侧 Broadcast 路由,W3 ④ 暂不订阅)
	TopicChatWorld = "pandora.chat.world"

	// TopicChatTeam — proto: pandora.chat.v1.ChatPushEvent
	// key=player_id;原则 2:只发收件方
	TopicChatTeam = "pandora.chat.team"

	// TopicChatPrivate — proto: pandora.chat.v1.ChatPushEvent
	// key=player_id;原则 2:只发接收方
	TopicChatPrivate = "pandora.chat.private"

	// TopicChatGuild — proto: pandora.chat.v1.ChatPushEvent
	// key=接收方 player_id;原则 2:只发收件方(公会频道 fan-out,即时不落库)
	TopicChatGuild = "pandora.chat.guild"

	// TopicChatGroup — proto: pandora.chat.v1.ChatPushEvent
	// key=接收方 player_id;原则 2:只发收件方(临时群频道 fan-out,即时不落库)
	TopicChatGroup = "pandora.chat.group"

	// TopicPlayerUpdate — proto: pandora.player.v1.PlayerUpdateEvent(W3+ 补)
	// key=player_id;玩家档案变更通知(MMR/昵称/英雄池)。
	//
	// ⚠️ 单事件类型 topic(金丝雀混跑安全,不变量 §21):player 服务旧副本消费本 topic 时
	// 不看 event_type header,直接按 PlayerUpdateEvent 解码做 MMR 入账 —— 在共存窗口里
	// 任何"同 topic 加新 event_type"的消息都会被旧副本误解码(字段 2/3 恰好能对上
	// match_id/mmr_delta,静默污染 MMR)。因此本 topic 永远只承载 PlayerUpdateEvent;
	// player 域新增事件一律开新 topic(如 TopicPlayerExperience)。
	TopicPlayerUpdate = "pandora.player.update"

	// TopicPlayerExperience — proto: pandora.player.v1.PlayerExperienceEvent(实时成长,2026-07-21)
	// key=player_id;player 服务经验入账后出箱生产,push 订阅透传客户端刷新经验条/播升级表现。
	// event_type header 恒为 PLAYER_PUSH_EVENT_TYPE_EXPERIENCE(客户端按 (topic, event_type) 选型)。
	// 独立 topic 的原因见 TopicPlayerUpdate 注释(旧 player 副本不订阅本 topic,混跑零风险)。
	TopicPlayerExperience = "pandora.player.experience"

	// TopicFriendEvent — proto: pandora.friend.v1.FriendEvent
	// key=to_player_id;原则 2:发给接收方(好友请求 / 接受通知)
	TopicFriendEvent = "pandora.friend.event"

	// TopicGuildEvent — proto: pandora.guild.v1.GuildEvent
	// key=to_player_id;原则 2:发给接收方(公会申请 / 审批 / 踢人 / 解散通知)
	TopicGuildEvent = "pandora.guild.event"

	// TopicSystemNotify — proto: pandora.system.v1.SystemNotifyEvent(W3+ 补)
	// 广播类(key 可空);系统公告 / 邮件红点 / 运营推送
	TopicSystemNotify = "pandora.system.notify"

	// TopicHubMigrate — proto: pandora.hub.v1.HubMigrateEvent
	// key=player_id;原则 2 例外:强制整合(缩容排空)时把「新分片地址+新 hub 票据+倒计时」
	// 推给被迁移玩家本人,客户端倒计时到点重连新大厅(与 Hub DS drain 心跳指令双通道)
	TopicHubMigrate = "pandora.hub.migrate"

	// TopicPresenceUpdate — proto: pandora.locator.v1.PresenceBatchEvent
	// key=subscriber_id;好友在线态订阅推送(docs/design/friend-distributed-scaling.md §13.4)。
	// player_locator 的 fan-out worker 去抖+合并后,把「你关注的好友 A/C/F 上线了」
	// 批量推给订阅者本人;push 服务按 key=subscriber_id 路由到其 stream。
	TopicPresenceUpdate = "pandora.presence.update"

	// TopicMissionUpdate — proto: pandora.mission.v1.MissionUpdateEvent(任务域,2026-08-11)
	// key=player_id;**原则 3 例外**:任务进度由 battle_result 出箱等异步事实驱动,
	// 状态机变化必须发给玩家本人。mission 服务事务出箱生产;推送不承担正确性
	// (原则 5):客户端判重按 mission_config_id + 状态,resync 回源 ListMissions。
	// 独立单事件类型 topic(金丝雀混跑纪律,见 TopicPlayerUpdate 注释)。
	TopicMissionUpdate = "pandora.mission.update"
)

// 非推送 topic(服务间事件,push 不订阅;W4 ③,2026-06-06)。
//
// 这两个不是给客户端推送用的,而是后端服务间异步事件:
//   - battle.result:战斗 DS 上报结算 → battle_result 幂等落库(key=match_id,不变量 §9)
//   - ds.lifecycle:ds_allocator 发 DS 生命周期事件(W4 ③ 仅 abandoned)→ battle_result 补偿
const (
	// TopicBattleResult — proto: pandora.battle.v1.BattleResult
	// key=match_id;at-least-once,消费者(battle_result)幂等落库(不变量 §2)
	TopicBattleResult = "pandora.battle.result"

	// TopicDSLifecycle — proto: pandora.ds.v1.DSLifecycleEvent
	// key=match_id;W4 ③ ds_allocator 心跳超时发 ABANDONED → battle_result 写补偿记录(不变量 §4)
	TopicDSLifecycle = "pandora.ds.lifecycle"

	// TopicPlayerPresence — proto: pandora.locator.v1.PlayerLeftHubEvent
	// key=player_id;player_locator 在 ReportDisconnect 成功缩 TTL 时生产。
	//
	// ⚠️ 语义是「离开了 Hub」而**不是「下线」**(travel 去战斗、秒重连都会产生),
	// 消费者只能把它当触发器,到自己的阈值后回查 locator 权威再动作(见 proto 注释)。
	// 供 pkg/offlinewatch 的通用消费骨架订阅;push 不订阅,不下发客户端。
	//
	// ⚠️ 勿与 TopicPresenceUpdate 混用:那条是给好友面板的粗粒度状态批量推送
	// (key=subscriber_id、单实例内存订阅 + 去抖、可 killswitch 丢弃),
	// 投递保证与语义都不同,混用会让「可丢的展示流」和「要兜底的业务触发流」互相污染。
	TopicPlayerPresence = "pandora.player.presence"
)

// BuildDLQTopic 构造死信队列 topic(infra.md §4.4),委托 config.BuildDLQTopic。
//
//	BuildDLQTopic("pandora.battle.result") → "pandora.dlq.battle.result"
func BuildDLQTopic(originalTopic string) string {
	return config.BuildDLQTopic(originalTopic)
}

// PushTopics 是 push 服务默认订阅的 topic 集合。
//
// W3 ④ 启用 team.update / match.progress / chat.private;
// 2026-06-15 friend 服务上线,补 friend.event(好友请求 / 接受推送)。
// 2026-06-16 chat 三频道补全:加 chat.team(队伍)/ chat.world(世界广播),
// 让队伍聊天和世界聊天也被 push 消费(此前只订阅 chat.private,team/world 消息丢失)。
// 2026-06-19 presence 订阅推送上线(§13.4),补 pandora.presence.update(好友在线态变更)。
// 2026-07-21 实时成长上线,补 pandora.player.experience(经验/升级推送,key=player_id)。
// 2026-08-11 任务域上线,补 pandora.mission.update(任务进度/完成/可领推送,key=player_id)。
// 后续 player.update / system.notify Event message 落地后,
// 在对应业务服 PR 里把常量加进本切片,push etc yaml 同步加 topics。
var PushTopics = []string{
	TopicTeamUpdate,
	TopicMatchProgress,
	TopicChatPrivate,
	TopicChatTeam,
	TopicChatWorld,
	TopicChatGuild,
	TopicChatGroup,
	TopicHubMigrate,
	TopicFriendEvent,
	TopicGuildEvent,
	TopicPresenceUpdate,
	TopicPlayerExperience,
	TopicMissionUpdate,
}

// BroadcastTopics 是「广播类」push topic 集合:这些 topic 的 kafka key 为空(广播语义),
// push 消费侧必须走 ConnectionManager.Broadcast 给全部在线玩家,而**不能**按 player_id key 解析
// (空 key ParseUint 会失败被当 invalid key ack 丢弃)。
//
// 目前包含 chat.world(世界聊天)/ system.notify(系统公告)。
// 其余 topic(team.update / match.progress / chat.private / chat.team / friend.event / hub.migrate)
// 都是 per-player 定向推送,key=player_id,走 SendTo。
var BroadcastTopics = map[string]struct{}{
	TopicChatWorld:    {},
	TopicSystemNotify: {},
}

// IsBroadcastTopic 判断一个 topic 是否为广播类(走 Broadcast 而非 SendTo)。
func IsBroadcastTopic(topic string) bool {
	_, ok := BroadcastTopics[topic]
	return ok
}
