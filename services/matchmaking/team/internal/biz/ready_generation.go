// ready_generation.go — 「谁准备好了」的单调代际(INC-20260813-001 ①)。
//
// # 它解决什么
//
// EndTeamMatch 是一次性调用,而 outbox 会重投。没有代际时,一条**迟到的旧局释放**
// 会把**下一局**(或玩家刚重新点上)的准备状态清掉:
//
//	对局结束 → EndTeamMatch → ACK 丢了 → outbox 保留任务
//	         → 期间玩家重新点准备 / 离队重入 / 队长已开新局
//	         → outbox 重投 → 照样把 ready 抹平
//
// 代际把「复位」从「按名字找人」变成「按版本 CAS」:重投带的 expected 对不上当前代际,直接 no-op。
//
// # 为什么用包装器自动推进,而不是在每处写点里手动 ++
//
// 「改了 ready 意图就要推进代际」这件事有 **7 处以上**写点(SetReady / 入队 / 离队 / 踢人 /
// 掉线软化 / 离线摘人 / 对局结束复位),散落在两个文件。漏掉任何一处的后果是**静默的** ——
// 代际停在旧值,CAS 照样通过,幂等保护形同虚设,而所有测试照样绿。
// 这正是本事故本身的失效形状(§14「不准留半成品」的反面教材)。
//
// 因此改成:**所有队伍写都必须走 updateTeam**,它在锁内比较前后指纹,变了就推进代际。
// 忘记推进在结构上不可能;而「绕过包装器直接调 repo.UpdateWithLock」由契约测试机械拦住
// (见 ready_generation_contract_test.go)。
package biz

import (
	"context"
	"strconv"
	"strings"
	"time"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// readyIntentFingerprint 概括「此刻谁准备好了」的**全部**可见事实。
//
// 只含三样:队伍状态、成员集合、每个成员的 ready 位。刻意**不含** map_id / 队长 / 昵称 /
// 英雄 —— 那些变了不影响「这一局该不该被复位」,把它们算进来只会让代际无谓地涨,
// 使正常的 EndTeamMatch 频繁 CAS 失败(该复位的反而不复位了)。
//
// 成员按 player_id 排序后拼接:Members 的存储顺序会因增删而变,不排序会把「顺序变了」
// 误判成「意图变了」。
func readyIntentFingerprint(team *teamv1.TeamStorageRecord) string {
	if team == nil {
		return ""
	}
	ids := make([]uint64, 0, len(team.Members))
	ready := make(map[uint64]bool, len(team.Members))
	for _, m := range team.Members {
		ids = append(ids, m.GetPlayerId())
		ready[m.GetPlayerId()] = m.GetReady()
	}
	// 队伍最多 5 人,插入排序足够且不引依赖。
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(team.GetState())))
	for _, id := range ids {
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(id, 10))
		if ready[id] {
			b.WriteString(":1")
		} else {
			b.WriteString(":0")
		}
	}
	return b.String()
}

// updateTeam 是 biz 层**唯一**允许的队伍写入口。
//
// 它在同一把乐观锁内做三件事:①快照 ready 意图指纹 → ②跑业务回调 → ③指纹变了就推进代际。
// 因为推进发生在锁内、与业务写同一次提交,代际与状态永远不会劈叉。
//
// fn 返回错误时整次写被放弃(UpdateWithLock 语义),代际自然也不会动。
func (u *TeamUsecase) updateTeam(
	ctx context.Context,
	teamID uint64,
	fn func(*teamv1.TeamStorageRecord) error,
	ttl time.Duration,
) error {
	return u.updateTeamThenStamp(ctx, teamID, fn, nil, ttl)
}

// updateTeamThenStamp 与 updateTeam 相同,但多一个在**代际推进之后**运行的 stamp 回调。
//
// # 为什么需要「之后」这个位置
//
// BeginTeamMatch 要在同一次提交里写一张收据,而收据必须记下**消费之后**的代际
// (它是「这是不是同一次尝试的重试」的 CAS 依据,见 MatchStartReceipt)。
// 业务回调 fn 跑的时候代际还没推进,在那里写收据只能靠「当前值 + 1」去猜 ——
// 那是在复刻 updateTeam 的内部实现,哪天推进规则一变(比如某类写不再推进),
// 收据就会静默记错一个永远对不上的值,而重入判定失效是**不报错**的。
//
// stamp 拿到的是本次提交的最终状态,它只该做「盖章」——不得再改 ready 意图
// (那会让指纹与已经算完的代际劈叉)。调用方只有 BeginTeamMatch 一个。
func (u *TeamUsecase) updateTeamThenStamp(
	ctx context.Context,
	teamID uint64,
	fn func(*teamv1.TeamStorageRecord) error,
	stamp func(*teamv1.TeamStorageRecord),
	ttl time.Duration,
) error {
	return u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		before := readyIntentFingerprint(team)
		if err := fn(team); err != nil {
			return err
		}
		if readyIntentFingerprint(team) != before {
			team.ReadyGeneration++
		}
		if stamp != nil {
			stamp(team)
		}
		return nil
	}, ttl)
}
