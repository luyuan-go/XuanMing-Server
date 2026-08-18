// source_revision.go — Hub assignment 来源版本(source revision,INC-20260818-003)。
//
// 解决的问题:滚动升级期新旧 hub_allocator 副本共存时,Owner 无法判断两份「target 不同」
// 的 Begin 哪一份来自更新的 assignment。反例(事故文档 §1)是确定性的:旧 binary 在
// assignment CAS **之前**执行 Owner Begin,conflict 后不复核当前 assignment 就拿新 epoch
// 盲写旧 target,于是 Redis=B 而 Owner=A2。owner_epoch 只能证明「谁后提交」,证明不了
// 「谁的来源更新」——这正是本文件要补上的那一维。
//
// 为什么现有字段都不行(事故文档 §2 已逐个否掉,别再试):
//   - assignment_id / auth_jti 是随机 UUID:唯一但**不可排序**;
//   - assigned_at_ms 是墙钟:回拨与同毫秒碰撞都会破坏全序;
//   - auth_epoch / auth_gen 是 per-pod 凭据版本:跨 Pod 无序;
//   - writer_token 只按 allocator **任期**递增:同一任期内的多次 assignment 完全相同,
//     而事故反例里 R1/R2 恰恰同任期;
//   - owner_epoch 是提交后的版本,倒果为因。
//
// ── 编码 ────────────────────────────────────────────────────────────────────
//
// source revision = 写者任期(高 40 位) × 2^24 + 任期内序号(低 24 位)
//
//	┌──────────────── 40 bit ────────────────┬───── 24 bit ─────┐
//	│ writer term(etcd leader CreateRevision)│ 任期内严格递增序号 │
//	└────────────────────────────────────────┴──────────────────┘
//
// 全序从两段各自的单调性推出:
//   - 高位:writerlease 的 token = 本届 leader key 的 CreateRevision,etcd 保证**历届严格
//     递增**(见 pkg/dsauthfence/writerlease 头注释);
//   - 低位:同一任期只有一个写者进程在铸号,进程内原子自增即严格递增。
//
// 于是任意两个 revision 可比,且「更大 = 来源更新」在跨任期、跨进程重启下都成立。
// 这正是 Owner 高水位需要的唯一性质。**不需要**额外的持久发号器:任期号本身已经是持久的
// (etcd revision 单调不回退),进程崩溃重启会拿到**更大**的任期号,低位从 0 重来也不会
// 与旧任期的号相撞。
//
// ── 为什么低位取 24 bit ──────────────────────────────────────────────────────
//
// 低位只需覆盖**单个任期内**的 target-changing assignment 次数。16_777_215 次对一届
// leader 而言极其宽裕(TTL 刷新、凭据轮换、cleanup-only CAS 都**不**领号,见事故文档 §3)。
// 高位留 40 bit = 1.1e12 个 etcd revision,同样远超真实集群寿命。两边都溢出即 fail-closed
// (Compose 返回错误),绝不静默回绕——回绕会让一个旧来源看起来更新,那正是本机制要防的事。
package placement

import "github.com/luyuancpp/pandora/pkg/errcode"

const (
	// SourceRevisionSeqBits 低位(任期内序号)占的位宽。
	SourceRevisionSeqBits = 24
	// SourceRevisionMaxSeq 单个写者任期内可铸的最大序号(含)。超出即 fail-closed。
	SourceRevisionMaxSeq = uint64(1)<<SourceRevisionSeqBits - 1
	// SourceRevisionMaxTerm 可编码的最大写者任期号(含)。超出即 fail-closed。
	SourceRevisionMaxTerm = uint64(1)<<(64-SourceRevisionSeqBits) - 1

	// SourceRevisionLegacy 是「本次 Begin 来自尚未滚上本协议的旧写者」的哨兵值。
	//
	// 0 不是「最小的合法版本」而是「没有版本」:它与任何非零 revision 都**不可比**。
	// Owner 对它的处置见 owner_repo.go 的 classifySourceRevision——一旦某玩家见过非零
	// revision,该玩家永久拒绝 legacy,否则旧写者可以靠「我不带版本」绕过整道门。
	SourceRevisionLegacy uint64 = 0
)

// ComposeSourceRevision 把(写者任期, 任期内序号)编成一个可全序比较的 revision。
//
// seq 必须由调用方在**同一任期内严格递增**地给出(通常是进程内 atomic.AddUint64 的返回值,
// 从 1 开始;0 留给 legacy 哨兵,故 seq=0 非法)。term 为 0 表示写者未持有租约,此时根本
// 不该铸号,同样按非法处理——没有任期就没有全序,铸出来的号无法与他人比较。
//
// 任何一侧越界都返回错误而不是截断 / 回绕:铸不出号时正确的动作是**拒绝这次 assignment**
// (fail-closed),不是发一个可能比旧号还小的版本出去。
func ComposeSourceRevision(writerTerm, seq uint64) (uint64, error) {
	if writerTerm == 0 {
		return 0, errcode.New(errcode.ErrInvalidState,
			"source revision needs a writer term; term=0 means this replica holds no writer lease")
	}
	if writerTerm > SourceRevisionMaxTerm {
		return 0, errcode.New(errcode.ErrInvalidState,
			"writer term %d exceeds source revision encoding limit %d", writerTerm, SourceRevisionMaxTerm)
	}
	if seq == 0 {
		return 0, errcode.New(errcode.ErrInvalidState,
			"source revision seq must start at 1; 0 is reserved for the legacy sentinel")
	}
	if seq > SourceRevisionMaxSeq {
		return 0, errcode.New(errcode.ErrInvalidState,
			"source revision seq %d exhausted for term %d (max %d); step down and re-elect to get a fresh term",
			seq, writerTerm, SourceRevisionMaxSeq)
	}
	return writerTerm<<SourceRevisionSeqBits | seq, nil
}

// SplitSourceRevision 拆回(任期, 序号)。只用于日志与排障——判定新旧一律直接比 revision
// 本身,不要拆开来比:拆开比会引入「同任期时才比 seq」这类分支,而全序本来就不需要分支。
func SplitSourceRevision(revision uint64) (writerTerm, seq uint64) {
	return revision >> SourceRevisionSeqBits, revision & SourceRevisionMaxSeq
}
