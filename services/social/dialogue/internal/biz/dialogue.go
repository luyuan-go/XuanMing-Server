// Package biz 是 dialogue 服务的业务逻辑层(2026-06-16)。
//
// 职责(docs/design/go-services.md §2.10):NPC 对话树运行时。
//   - StartDialogue:按 npc_id 取对话树,创建服务端会话,返回起始节点 DialogueState
//   - ChooseOption:校验选项合法性 + 前置可见性,推进会话到下一节点
//   - EndDialogue:结束并回收会话(幂等)
//
// 安全规则:
//   - 对话树是服务端权威配置;客户端只渲染 DialogueState,选择只回传 option_id。
//   - dialogue_id 由 snowflake 生成(服务端持有),会话归属用 player_id 校验(R5:
//     player_id 来自 JWT ctx),非本人会话一律按「不存在」处理,不泄露他人会话。
//   - 选项可见性(DialogueOption.visible)在服务端判定;不可见选项即使客户端回传
//     其 option_id 也拒绝(ErrDialogueOptionInvalid)。
//
// 当前最小版本:简单 if-else 对话树,选项无副作用(领取奖励 / 改任务进度等留后续
// 接 trade / player 服务);可见性为静态配置(基于玩家数据的条件判定留后续)。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	dialoguev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/dialogue/v1"

	"github.com/luyuancpp/pandora/services/social/dialogue/internal/data"
)

// DialogueUsecase 是 dialogue 服务业务逻辑核心。
type DialogueUsecase struct {
	trees      data.TreeProvider
	sessions   data.SessionStore
	sessionTTL time.Duration

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,会话 owner 落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,StartDialogue 会话创建成功后额外打一条会话
	// owner 落点观测(核对会话落点 == 玩家 owner cell)。nil-safe。
	router *cellroute.Router
}

// NewDialogueUsecase 构造。sessionTTL <= 0 时回退到 5m。
func NewDialogueUsecase(trees data.TreeProvider, sessions data.SessionStore, sessionTTL time.Duration) *DialogueUsecase {
	if sessionTTL <= 0 {
		sessionTTL = 5 * time.Minute
	}
	return &DialogueUsecase{trees: trees, sessions: sessions, sessionTTL: sessionTTL}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),StartDialogue 不做会话 owner 落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result / friend / chat / trade 一致)。Router 内部读路径无锁,并发安全。
func (u *DialogueUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// StartDialogue 开启一次 NPC 对话。newDialogueID 由 service 用 snowflake 预生成。
func (u *DialogueUsecase) StartDialogue(ctx context.Context, playerID uint64, npcID uint32, newDialogueID uint64) (*dialoguev1.DialogueState, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if npcID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "npc_id required")
	}
	if newDialogueID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "dialogue_id required")
	}

	tree, ok := u.trees.GetTree(npcID)
	if !ok {
		return nil, errcode.New(errcode.ErrDialogueNotFound, "no dialogue tree for npc %d", npcID)
	}
	node, ok := tree.Nodes[tree.StartNode]
	if !ok {
		// 配置错误:起始节点不存在。
		return nil, errcode.New(errcode.ErrDialogueNotFound, "start node %q missing for npc %d", tree.StartNode, npcID)
	}

	now := nowMs()
	s := &data.Session{
		DialogueID: newDialogueID,
		PlayerID:   playerID,
		NpcID:      npcID,
		NodeID:     tree.StartNode,
		CreatedMs:  now,
		ExpiresMs:  now + u.sessionTTL.Milliseconds(),
	}
	created, err := u.sessions.Create(ctx, s)
	if err != nil {
		return nil, err
	}
	if !created {
		// snowflake 预生成的 dialogue_id 已被占用 = 唯一键冲突,几乎只可能由 snowflake node_id
		// 重号(多副本配置错)引起;对客户端屏蔽为 NotFound,服务端 WARN 暴露真实原因。
		plog.With(ctx).Warnw("msg", "dialogue_id_conflict",
			"dialogue_id", newDialogueID, "player_id", playerID, "npc_id", npcID)
		return nil, errcode.New(errcode.ErrDialogueNotFound, "dialogue_id %d already in use", newDialogueID)
	}

	// 分片:会话创建成功后观测本会话锁定的 owner 落点(会话是玩家 owner 数据,须锁定
	// 玩家 owner cell,scale-cellular-20m.md §4.2)。router 为 nil(单 Cell)→ 不打。
	u.logSessionPlacement(ctx, newDialogueID, playerID)

	state := buildState(newDialogueID, tree, node)
	// 起始节点即终止节点(无可见选项)→ 对话立即结束,回收会话。
	if state.GetEnded() {
		_ = u.sessions.Delete(ctx, newDialogueID)
	}
	return state, nil
}

// ChooseOption 选择一个选项,推进会话到下一节点。
func (u *DialogueUsecase) ChooseOption(ctx context.Context, playerID, dialogueID uint64, optionID string) (*dialoguev1.DialogueState, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if dialogueID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "dialogue_id required")
	}
	if optionID == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "option_id required")
	}

	s, found, err := u.sessions.Get(ctx, dialogueID, nowMs())
	if err != nil {
		return nil, err
	}
	// R5:非本人会话按不存在处理,不泄露他人会话。
	if !found || s.PlayerID != playerID {
		if found && s.PlayerID != playerID {
			// 有人拿 dialogue_id 访问他人会话(IDOR / 越权探测);对客户端屏蔽为 NotFound,
			// 服务端 WARN 留证,否则无法发现有人在猜别人的 dialogue_id。
			plog.With(ctx).Warnw("msg", "dialogue_cross_player_access",
				"dialogue_id", dialogueID, "caller_id", playerID, "owner_id", s.PlayerID)
		}
		return nil, errcode.New(errcode.ErrDialogueNotFound, "dialogue %d not found", dialogueID)
	}

	tree, ok := u.trees.GetTree(s.NpcID)
	if !ok {
		// 活跃会话的对话树消失 = 滚动更新换配置 / 配置漂移;对客户端是 NotFound,服务端 WARN 暴露内部一致性。
		plog.With(ctx).Warnw("msg", "dialogue_tree_missing_active_session",
			"dialogue_id", dialogueID, "npc_id", s.NpcID)
		return nil, errcode.New(errcode.ErrDialogueNotFound, "no dialogue tree for npc %d", s.NpcID)
	}
	node, ok := tree.Nodes[s.NodeID]
	if !ok {
		// 会话推进到不存在的节点 = 配置漂移或内部 bug,同上 WARN 暴露。
		plog.With(ctx).Warnw("msg", "dialogue_node_missing_active_session",
			"dialogue_id", dialogueID, "npc_id", s.NpcID, "node_id", s.NodeID)
		return nil, errcode.New(errcode.ErrDialogueNotFound, "node %q missing for npc %d", s.NodeID, s.NpcID)
	}

	// 选项必须存在且可见(不可见选项即使客户端回传也拒绝)。
	chosen := findVisibleOption(node, optionID)
	if chosen == nil {
		return nil, errcode.New(errcode.ErrDialogueOptionInvalid, "option %q invalid at node %q", optionID, s.NodeID)
	}

	next, ok := tree.Nodes[chosen.NextNode]
	if chosen.NextNode == "" || !ok {
		// 选项无后续节点 → 对话结束,回收会话。
		_ = u.sessions.Delete(ctx, dialogueID)
		return endedState(dialogueID, tree), nil
	}

	s.NodeID = chosen.NextNode
	if err := u.sessions.Update(ctx, s); err != nil {
		return nil, err
	}

	state := buildState(dialogueID, tree, next)
	// 跳转到的节点是终止节点 → 展示其文本后对话结束,回收会话。
	if state.GetEnded() {
		_ = u.sessions.Delete(ctx, dialogueID)
	}
	return state, nil
}

// EndDialogue 结束对话,回收会话(幂等:会话不存在 / 非本人均返回成功)。
func (u *DialogueUsecase) EndDialogue(ctx context.Context, playerID, dialogueID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if dialogueID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "dialogue_id required")
	}
	s, found, err := u.sessions.Get(ctx, dialogueID, nowMs())
	if err != nil {
		return err
	}
	// 仅回收本人会话;非本人 / 不存在不报错(幂等结束语义)。
	if found && s.PlayerID == playerID {
		return u.sessions.Delete(ctx, dialogueID)
	}
	return nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// buildState 把对话树节点渲染成客户端可见的 DialogueState。
// 只输出可见选项;无可见选项即为终止节点(ended=true)。
func buildState(dialogueID uint64, tree *data.DialogueTree, node *data.DialogueNode) *dialoguev1.DialogueState {
	opts := make([]*dialoguev1.DialogueOption, 0, len(node.Options))
	for i := range node.Options {
		o := &node.Options[i]
		if !o.Visible {
			continue
		}
		opts = append(opts, &dialoguev1.DialogueOption{
			OptionId: o.OptionID,
			Text:     o.Text,
			Visible:  true,
		})
	}
	speaker := node.Speaker
	if speaker == "" {
		speaker = tree.Speaker // 节点未单独指定说话人 → 用该 NPC 的主名
	}
	return &dialoguev1.DialogueState{
		DialogueId: dialogueID,
		NpcId:      tree.NpcID,
		NodeId:     node.NodeID,
		Speaker:    speaker,
		Text:       node.Text,
		Options:    opts,
		Ended:      len(opts) == 0,
	}
}

// endedState 是「选项无后续节点」时返回的结束态(无当前节点文本)。
func endedState(dialogueID uint64, tree *data.DialogueTree) *dialoguev1.DialogueState {
	return &dialoguev1.DialogueState{
		DialogueId: dialogueID,
		NpcId:      tree.NpcID,
		Speaker:    tree.Speaker,
		Ended:      true,
	}
}

// findVisibleOption 在节点里找可见且 id 匹配的选项;找不到返回 nil。
func findVisibleOption(node *data.DialogueNode, optionID string) *data.DialogueOption {
	for i := range node.Options {
		o := &node.Options[i]
		if o.OptionID == optionID && o.Visible {
			return o
		}
	}
	return nil
}

// nowMs 返回当前毫秒时间戳。
func nowMs() int64 {
	return time.Now().UnixMilli()
}
