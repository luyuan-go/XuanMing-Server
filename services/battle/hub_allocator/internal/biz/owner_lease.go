// owner_lease.go — owner 权威实例租约双写(owner-authority.md migrate ⑥,2026-07-22)。
//
// Hub DS 的 Model B 授权心跳成功后,把该实例的租约代写进 owner 权威(ds_instance_lease):
// 玩家级 owner lease 由实例租约派生,BeginTransition 的 admit_not_before 屏障据此计算。
// hub 凭据不携带实例纪元 → 续租 instance_epoch 传 0(owner 侧仅在双方都非零且不同时拒)。
package biz

import (
	"context"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// ownerLeaseWeakLog / ownerLeaseLogWindowMs:租约双写弱依赖失败的日志限流(模式 C)。
// 包级变量:该信号是**进程级依赖健康度**(owner 权威可达性),不需要按实例分桶;
// plog.Window 无 key、常量内存、并发安全。
var ownerLeaseWeakLog plog.Window

const ownerLeaseLogWindowMs = 5000

// OwnerLeaseRenewer 把已授权 DS 实例心跳代写进 owner 权威的实例租约。
// 由 owner 服务 gRPC 客户端实现;可为 nil(未配 owner_addr → 不双写,migrate 前行为不变)。
type OwnerLeaseRenewer interface {
	RenewInstanceLease(ctx context.Context, podName, instanceUID string, instanceEpoch uint32, releaseTrack string) error
}

// SetOwnerLeaseRenewer 注入 owner 租约续写器(nil-safe)。required 语义见 renewOwnerLeaseGate。
func (u *HubUsecase) SetOwnerLeaseRenewer(r OwnerLeaseRenewer, required bool) {
	u.ownerLease = r
	u.ownerLeaseRequired = required
}

// renewOwnerLeaseGate 心跳响应返回前的租约双写门。
//
// 时序约束(owner-authority.md §4):双写必须发生在心跳响应返回**之前**——DS 收到响应
// 才会延长本地租约,权威侧 lease 必须先覆盖该认知,否则 BeginTransition 在屏障计算时
// 可能观察到偏小的旧 deadline。
//   - renewer nil → no-op(未启用);
//   - 失败 + !required(migrate 弱依赖,默认)→ 告警放行,由旧 last_heartbeat_ms 再入门
//     (placement.DSFenceReentryBarrier)双门并行兜底;
//   - 失败 + required(contract 强依赖)→ 心跳失败:DS 拿不到响应就不会延长本地租约,
//     连续失败按 fence 契约自我 fencing——权威侧租约滞后时 DS 也必然停玩,时序仍闭合。
func renewOwnerLeaseGate(ctx context.Context, renewer OwnerLeaseRenewer, required bool,
	podName, instanceUID string, instanceEpoch uint32, releaseTrack string) error {
	if renewer == nil {
		return nil
	}
	err := renewer.RenewInstanceLease(ctx, podName, instanceUID, instanceEpoch, releaseTrack)
	if err == nil {
		return nil
	}
	if required {
		return err
	}
	// 模式 C:本门在**每次授权心跳**返回前执行(每 pod ~5s),owner 权威抖动时按 pod 数刷屏,
	// 而这是被双门(旧 last_heartbeat_ms 再入门)兜住的弱依赖 → 首错 + 每窗口一条 + 累计数。
	if ok, streak := ownerLeaseWeakLog.Admit(time.Now().UnixMilli(), ownerLeaseLogWindowMs); ok {
		plog.With(ctx).Warnw("msg", "owner_lease_renew_failed_weak",
			"pod", podName, "uid", instanceUID, "epoch", instanceEpoch,
			"streak", streak, "err", err)
	}
	return nil
}
