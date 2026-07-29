// Package dsauthfence 为 DS 回调授权协议提供不可回退的进程级激活栅栏。
//
// Redis 的每实例 required_writer_epoch 负责数据面的最终拒绝，本包负责控制面：
// 进程启动时线性读取 etcd 全局 required epoch，注册带租约的 capability，并持续 watch。
// 初读失败、required 回退/删除、未来 epoch 或租约丢失都会关闭 Lost；调用方必须立即退出。
package dsauthfence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var capabilityFeaturePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{2,63}$`)

// ErrTopologyChangeLockProviderUnavailable is a release blocker, not a
// retryable etcd failure. No provider API/trust root exists in this repository
// that can prevent Redis failover, reshard, or atomic slot migration across
// the full preflight-to-CAS window.
var ErrTopologyChangeLockProviderUnavailable = errors.New(
	"dsauthfence: authoritative Redis topology-change lock provider is not wired; target epoch CAS is disabled")

const (
	// ProtocolEpochV2 是 Redis active/pending 完整凭据协议版本。
	ProtocolEpochV2 uint32 = 2
	// RequiredPolicyV2 is part of the etcd required value, not deployment
	// configuration.  An old epoch-2 binary only understands the numeric value
	// "2" and therefore fails closed when it sees RequiredValueV2.
	RequiredPolicyV2 = "ds-auth-v2-pod-uid-write-invariant-v1"
	RequiredValueV2  = "2@" + RequiredPolicyV2
	// RequiredPolicyV3 advances the immutable control-plane policy while the
	// Redis/data-plane writer protocol remains epoch 2.  Keeping a new raw value
	// (rather than mutating V2) is the durable rollback fence: a V2 binary cannot
	// parse it and therefore cannot reacquire a writer capability.
	RequiredPolicyV3           = "ds-auth-v2-hub-successor-lease-v1"
	RequiredValueV3            = "2@" + RequiredPolicyV3
	RequiredPolicyGenerationV1 = uint32(1)
	RequiredPolicyGenerationV2 = uint32(2)
	RequiredPolicyGenerationV3 = uint32(3)
	// DefaultPrefix 是全局 required 与 capability key 的根前缀。
	DefaultPrefix = "/pandora/ds-auth/"
	// DefaultLeaseTTLSec 是 capability 租约 TTL。
	DefaultLeaseTTLSec int64 = 15
	// DefaultDialTimeout 是 etcd 启动期操作超时。
	DefaultDialTimeout = 5 * time.Second
	// EnvPodUID / EnvImageDigest 必须由 K8s Downward API 注入并由准入策略与 Pod 实体绑定。
	EnvPodUID      = "PANDORA_POD_UID"
	EnvImageDigest = "PANDORA_IMAGE_DIGEST"
)

// requiredPolicyV2Features is the complete production AcquireRuntime writer
// set. Keep it exact: adding a service or feature requires a new policy ID,
// otherwise an old binary could silently rejoin under an existing policy.
var requiredPolicyV2Features = map[string][]string{
	"login":          {},
	"player_locator": {},
	"ds_allocator": {
		"battle-release-expected-tuple-v1",
		"battle-storage-pod-uid-write-invariant-v1",
	},
	"hub_allocator": {
		"hub-reservation-ledger-v1",
		"hub-heartbeat-capacity-v1",
		"hub-owner-cleanup-v1",
		"hub-physical-eviction-v1",
	},
	"battle_result": {"battle-terminal-outbox-v1"},
}

// requiredPolicyV3Features is a new immutable policy set.  Do not add the new
// feature to requiredPolicyV2Features: doing so would let the same etcd value
// mean different things to different binaries and reopen rollback.
var requiredPolicyV3Features = map[string][]string{
	"login":          {},
	"player_locator": {},
	"ds_allocator": {
		"battle-release-expected-tuple-v1",
		"battle-storage-pod-uid-write-invariant-v1",
	},
	"hub_allocator": {
		"hub-reservation-ledger-v1",
		"hub-heartbeat-capacity-v1",
		"hub-owner-cleanup-v1",
		"hub-physical-eviction-v1",
		"hub-successor-lease-v1",
	},
	"battle_result": {"battle-terminal-outbox-v1"},
}

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Config 是单个进程注册 capability 所需的不可伪造运行时信息。
type Config struct {
	Endpoints      []string
	Prefix         string
	Service        string
	InstanceUID    string
	ImageDigest    string
	KeysetRevision string
	WriterEpoch    uint32
	LeaseTTLSec    int64
	DialTimeout    time.Duration
	Security       ClientSecurity
	Features       []string
}

// RuntimeConfig 是业务 main 已有配置可直接映射的部分；不可变 Pod 身份只从环境读取。
type RuntimeConfig struct {
	Endpoints      []string
	Prefix         string
	Service        string
	KeysetRevision string
	LeaseTTLSec    int64
	DialTimeout    time.Duration
	WriterEpoch    uint32
	Features       []string
}

// AcquireRuntime 从 Downward API 环境组装完整身份并启动 fence；绝不回退 hostname 或 image tag。
func AcquireRuntime(ctx context.Context, runtime RuntimeConfig) (*Holder, error) {
	security, err := ClientSecurityFromEnv()
	if err != nil {
		return nil, err
	}
	return Acquire(ctx, Config{
		Endpoints:      runtime.Endpoints,
		Prefix:         runtime.Prefix,
		Service:        runtime.Service,
		InstanceUID:    os.Getenv(EnvPodUID),
		ImageDigest:    os.Getenv(EnvImageDigest),
		KeysetRevision: runtime.KeysetRevision,
		WriterEpoch:    runtime.WriterEpoch,
		LeaseTTLSec:    runtime.LeaseTTLSec,
		DialTimeout:    runtime.DialTimeout,
		Security:       security,
		Features:       runtime.Features,
	})
}

// Capability 是写入 etcd lease key 的审计记录。
type Capability struct {
	Service                   string   `json:"service"`
	InstanceUID               string   `json:"instance_uid"`
	WriterEpoch               uint32   `json:"writer_epoch"`
	SupportedPolicyGeneration uint32   `json:"supported_policy_generation"`
	SupportedPolicyID         string   `json:"supported_policy_id"`
	AcquiredPolicyGeneration  uint32   `json:"acquired_policy_generation"`
	AcquiredPolicyID          string   `json:"acquired_policy_id,omitempty"`
	ImageDigest               string   `json:"image_digest"`
	KeysetRevision            string   `json:"keyset_revision"`
	EtcdIdentityRevision      string   `json:"etcd_identity_revision,omitempty"`
	StartedAtMs               int64    `json:"started_at_ms"`
	Features                  []string `json:"features,omitempty"`
}

// RequiredEvent 是 required key 的有序 watch 事件。
type RequiredEvent struct {
	State    RequiredState
	Revision int64
	Deleted  bool
	Err      error
}

// RequiredState is the canonical, versioned etcd fencing value. RawValue is
// compared byte-for-byte in the capability registration transaction.
type RequiredState struct {
	Epoch            uint32
	PolicyGeneration uint32
	PolicyID         string
	RawValue         string
}

// Lease 是 capability 的存活权。Lost 关闭后进程不得继续处理受保护写请求。
type Lease interface {
	Lost() <-chan struct{}
	Close() error
}

// Backend 隔离 etcd 细节，允许用确定性 fake 验证所有 fail-closed 分支。
type Backend interface {
	GetRequired(context.Context, string) (state RequiredState, watchRevision int64, modRevision int64, found bool, err error)
	// GetCapability 线性读取本进程 capability key 现值(同 Pod 崩溃重启的安全接管预检)。
	GetCapability(ctx context.Context, key string) (value []byte, modRevision int64, leaseID int64, found bool, err error)
	// AcquireCapability 注册 capability。prevModRevision==0 要求 key 不存在
	// (CreateRevision==0);>0 表示同身份安全接管:CAS ModRevision 精确匹配才原子替换,
	// 成功后 best-effort revoke prevLeaseID(终结旧 keepalive,不影响已挂新租约的 key)。
	AcquireCapability(ctx context.Context, key, lockKey, requiredKey string,
		expectedRequiredValue string, expectedRequiredModRevision int64,
		value []byte, ttl int64, prevModRevision int64, prevLeaseID int64) (Lease, error)
	WatchRequired(context.Context, string, int64) <-chan RequiredEvent
	Close() error
}

// Holder 持有 capability 租约及进程内只增不减的 required 高水位。
type Holder struct {
	backend Backend
	lease   Lease
	cancel  context.CancelFunc

	required    atomic.Uint32
	policy      atomic.Uint32
	reclaimed   bool
	lost        chan struct{}
	lostReason  atomic.Value // string;与 lost 同一次 lostOnce 内写入,读者见 closed 即见 reason
	lostOnce    sync.Once
	closeOnce   sync.Once
	intentional atomic.Bool
}

// 失效原因常量(LostReason)。五个触发分支在最终日志里此前完全同形,事故取证
// (2026-07-29 ds_allocator 保护性退出)无法继续细分"是失租还是 watch 断",必须区分。
const (
	// LostReasonLeaseKeepaliveEnded:capability 租约 keepalive 通道结束
	// (etcd 断连 / 租约过期 / 被 revoke)。
	LostReasonLeaseKeepaliveEnded = "lease_keepalive_ended"
	// LostReasonRequiredWatchError:required 键 watch 返回错误(含 compact revision)。
	LostReasonRequiredWatchError = "required_watch_error"
	// LostReasonRequiredDeleted:required 键被删除或值为空,权威策略已不可证明。
	LostReasonRequiredDeleted = "required_deleted_or_empty"
	// LostReasonRequiredRegressed:revision / epoch / policy generation 回退,
	// 或新策略对本 capability 不再合法。
	LostReasonRequiredRegressed = "required_regressed_or_invalid"
	// LostReasonRequiredAdvanced:required epoch / policy 正常推进,按契约强制重启
	// 以便用新 raw value 重新 CAS 注册 capability(不是故障)。
	LostReasonRequiredAdvanced = "required_advanced"
	// LostReasonRequiredWatchClosed:watch 通道静默结束(未报错也未取消)。
	LostReasonRequiredWatchClosed = "required_watch_closed"
)

// Start 使用已构造的 Backend 启动栅栏。生产调用 Acquire；测试可注入 fake。
func Start(ctx context.Context, backend Backend, cfg Config) (*Holder, error) {
	if backend == nil {
		return nil, errors.New("dsauthfence: nil backend")
	}
	normalize(&cfg)
	if err := validate(cfg); err != nil {
		_ = backend.Close()
		return nil, err
	}

	requiredKey := requiredKey(cfg.Prefix)
	readCtx, cancelRead := context.WithTimeout(ctx, cfg.DialTimeout)
	required, revision, requiredModRevision, found, err := backend.GetRequired(readCtx, requiredKey)
	cancelRead()
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("dsauthfence: linearizable read required: %w", err)
	}
	if !found || required.Epoch == 0 || required.RawValue == "" {
		_ = backend.Close()
		return nil, errors.New("dsauthfence: required writer epoch missing; explicit bootstrap is required")
	}
	if required.Epoch > cfg.WriterEpoch {
		_ = backend.Close()
		return nil, fmt.Errorf("dsauthfence: required writer epoch %d exceeds supported %d", required.Epoch, cfg.WriterEpoch)
	}
	if err := validateRequiredPolicyForCapability(required, cfg.Service, cfg.WriterEpoch, cfg.Features); err != nil {
		_ = backend.Close()
		return nil, err
	}

	capability := Capability{
		Service:                   cfg.Service,
		InstanceUID:               cfg.InstanceUID,
		WriterEpoch:               cfg.WriterEpoch,
		SupportedPolicyGeneration: RequiredPolicyGenerationV3,
		SupportedPolicyID:         RequiredPolicyV3,
		AcquiredPolicyGeneration:  required.PolicyGeneration,
		AcquiredPolicyID:          required.PolicyID,
		ImageDigest:               cfg.ImageDigest,
		KeysetRevision:            cfg.KeysetRevision,
		EtcdIdentityRevision:      cfg.Security.IdentityRevision,
		StartedAtMs:               time.Now().UnixMilli(),
		Features:                  append([]string(nil), cfg.Features...),
	}
	payload, err := json.Marshal(capability)
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("dsauthfence: marshal capability: %w", err)
	}
	lease, reclaimed, err := acquireWithSamePodTakeover(ctx, backend, cfg, requiredKey, required.RawValue, requiredModRevision, payload)
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("dsauthfence: acquire capability: %w", err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	h := &Holder{backend: backend, lease: lease, cancel: cancel, reclaimed: reclaimed, lost: make(chan struct{})}
	h.required.Store(required.Epoch)
	h.policy.Store(required.PolicyGeneration)
	watch := backend.WatchRequired(watchCtx, requiredKey, revision+1)
	go h.monitorLease(lease.Lost())
	go h.monitorRequired(watch, cfg.Service, cfg.WriterEpoch, append([]string(nil), cfg.Features...),
		requiredModRevision, required.PolicyGeneration)
	return h, nil
}

// acquireWithSamePodTakeover 注册 capability,并在「同 Pod 上一进程 fatal 崩溃
// (无法执行 defer Close)残留同身份 capability」时做安全接管,消除等旧租约 TTL
// 自然过期的恢复空窗(§16.8:恢复最坏耗时不得吃光业务安全租约)。
//
// 接管不放宽单 writer / 防脑裂:
//   - key 按 (service, PodUID) 唯一,异 Pod 副本各持异 key,永不互相接管;
//   - 只有 validateSamePodTakeover 全部身份字段一致才接管;任何不一致 fail-closed;
//   - 接管是 ModRevision 精确 CAS(并发接管者最多一个成功)+ 与 required/activation
//     lock 同一事务判定,与全新注册走完全相同的 fencing 条件;
//   - 接管成功后 revoke 旧租约:若旧进程理论上仍存活,其 keepalive 立即终结 → Lost
//     触发退出,结构性保证不出现第二个自认持有 capability 的进程。
//
// 两次尝试仅覆盖「预检 Get 与注册 Txn 之间残留租约恰好自然过期/键变化」的窄竞态,
// 把一次容器级 CrashLoop 退避(≥10s)收敛为一次进程内重读。
func acquireWithSamePodTakeover(
	ctx context.Context,
	backend Backend,
	cfg Config,
	requiredKey, requiredRawValue string,
	requiredModRevision int64,
	payload []byte,
) (Lease, bool, error) {
	capKey := capabilityKey(cfg.Prefix, cfg.Service, cfg.InstanceUID)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		preCtx, cancelPre := context.WithTimeout(ctx, cfg.DialTimeout)
		prevValue, prevModRevision, prevLeaseID, prevFound, err := backend.GetCapability(preCtx, capKey)
		cancelPre()
		if err != nil {
			return nil, false, fmt.Errorf("linearizable read own capability: %w", err)
		}
		if prevFound {
			if terr := validateSamePodTakeover(prevValue, cfg); terr != nil {
				return nil, false, terr
			}
		} else {
			prevModRevision, prevLeaseID = 0, 0
		}
		leaseCtx, cancelLease := context.WithTimeout(ctx, cfg.DialTimeout)
		lease, err := backend.AcquireCapability(
			leaseCtx, capKey, activationLockKey(cfg.Prefix), requiredKey,
			requiredRawValue, requiredModRevision,
			payload, cfg.LeaseTTLSec, prevModRevision, prevLeaseID,
		)
		cancelLease()
		if err == nil {
			return lease, prevFound, nil
		}
		lastErr = err
	}
	return nil, false, lastErr
}

// validateSamePodTakeover 判定残留 capability 是否属于「同一 Pod 的上一个进程」。
// 同 PodUID ⇒ kubelet 串行重启同一容器 ⇒ 同一不可变 Pod spec(镜像 digest 一致)且
// 旧进程必已退出,接管不会产生第二 writer。任何字段不一致(异身份写入、镜像已换、
// 配置漂移)一律拒绝接管并 fail-closed,等旧租约自然过期或人工介入,不得放宽。
func validateSamePodTakeover(prevRaw []byte, cfg Config) error {
	var prev Capability
	if err := json.Unmarshal(prevRaw, &prev); err != nil {
		return fmt.Errorf("dsauthfence: stale capability unparsable, refuse takeover: %w", err)
	}
	if prev.Service != cfg.Service || prev.InstanceUID != cfg.InstanceUID {
		return fmt.Errorf("dsauthfence: stale capability identity %s/%s mismatch, refuse takeover",
			prev.Service, prev.InstanceUID)
	}
	if prev.ImageDigest != cfg.ImageDigest {
		return errors.New("dsauthfence: stale capability image digest mismatch, refuse takeover")
	}
	if prev.WriterEpoch != cfg.WriterEpoch {
		return errors.New("dsauthfence: stale capability writer epoch mismatch, refuse takeover")
	}
	if prev.KeysetRevision != cfg.KeysetRevision {
		return errors.New("dsauthfence: stale capability keyset revision mismatch, refuse takeover")
	}
	return nil
}

// RequiredEpoch 返回本进程已经观察到的全局单调高水位。
func (h *Holder) RequiredEpoch() uint32 { return h.required.Load() }

// Reclaimed 报告本次注册是否通过同 Pod 安全接管取得(供启动日志/审计观测)。
func (h *Holder) Reclaimed() bool { return h.reclaimed }

// RequiredPolicyGeneration is the immutable control-plane policy generation.
// V2 and V3 both require data-plane WriterEpoch=2.
func (h *Holder) RequiredPolicyGeneration() uint32 { return h.policy.Load() }

// Lost 在任何 fencing 条件失效时关闭。调用方必须停止服务并退出。
func (h *Holder) Lost() <-chan struct{} { return h.lost }

// LostReason 返回本次失效的分支标识(LostReason* 常量之一);尚未失效时返回 ""。
// 只在观察到 Lost() 已关闭后读取才有意义:reason 与 close 在同一次 lostOnce 内写,
// 且 Store 先于 close,故"见 closed ⇒ 见 reason"。
func (h *Holder) LostReason() string {
	if v, ok := h.lostReason.Load().(string); ok {
		return v
	}
	return ""
}

func (h *Holder) monitorLease(ch <-chan struct{}) {
	<-ch
	if !h.intentional.Load() {
		h.signalLost(LostReasonLeaseKeepaliveEnded)
	}
}

func (h *Holder) monitorRequired(ch <-chan RequiredEvent, service string, supported uint32, features []string,
	initialRevision int64, initialPolicyGeneration uint32) {
	seenRevision := initialRevision
	seenPolicyGeneration := initialPolicyGeneration
	for event := range ch {
		if h.intentional.Load() {
			return
		}
		if event.Err != nil {
			h.signalLost(LostReasonRequiredWatchError)
			return
		}
		if event.Deleted || event.State.Epoch == 0 || event.State.RawValue == "" {
			h.signalLost(LostReasonRequiredDeleted)
			return
		}
		seen := h.required.Load()
		if event.Revision <= seenRevision || event.State.Epoch < seen || event.State.Epoch > supported ||
			event.State.PolicyGeneration <= seenPolicyGeneration ||
			validateRequiredPolicyForCapability(event.State, service, supported, features) != nil {
			h.signalLost(LostReasonRequiredRegressed)
			return
		}
		// Capability acquisition is CAS-bound to the old raw required value.  Even
		// for the sole canonical V1→V2, V1→V3, or V2→V3 advance, force process
		// restart so the replacement capability is acquired against the new raw
		// value.  Continuing with an old lease would make the audit ambiguous.
		seenRevision = event.Revision
		seenPolicyGeneration = event.State.PolicyGeneration
		h.required.Store(event.State.Epoch)
		h.policy.Store(event.State.PolicyGeneration)
		h.signalLost(LostReasonRequiredAdvanced)
		return
	}
	if !h.intentional.Load() {
		// watch 静默结束也不能继续写；不以重连/旧缓存冒充授权。
		h.signalLost(LostReasonRequiredWatchClosed)
	}
}

// signalLost 幂等地记录原因并关闭 Lost。reason 必须先于 close 写入,保证任何
// 观察到 channel 已关闭的 goroutine 都能读到非空 reason(happens-before 由
// lostOnce 内的顺序 + channel close/receive 建立)。
func (h *Holder) signalLost(reason string) {
	h.lostOnce.Do(func() {
		h.lostReason.Store(reason)
		close(h.lost)
	})
}

// Close 主动释放 capability 并停止 watch。幂等；主动关闭不触发 Lost。
func (h *Holder) Close() error {
	var out error
	h.closeOnce.Do(func() {
		h.intentional.Store(true)
		if h.cancel != nil {
			h.cancel()
		}
		if h.lease != nil {
			out = h.lease.Close()
		}
		if err := h.backend.Close(); out == nil {
			out = err
		}
	})
	return out
}

func normalize(cfg *Config) {
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.LeaseTTLSec <= 0 {
		cfg.LeaseTTLSec = DefaultLeaseTTLSec
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
}

func validate(cfg Config) error {
	if len(cfg.Endpoints) == 0 {
		return errors.New("dsauthfence: empty endpoints")
	}
	if cfg.Service == "" || strings.Contains(cfg.Service, "/") {
		return errors.New("dsauthfence: invalid service")
	}
	if _, known := requiredPolicyV2Features[cfg.Service]; !known {
		return fmt.Errorf("dsauthfence: service %q is not in the production writer policy", cfg.Service)
	}
	if cfg.InstanceUID == "" || strings.Contains(cfg.InstanceUID, "/") {
		return errors.New("dsauthfence: invalid instance uid")
	}
	if cfg.WriterEpoch == 0 {
		return errors.New("dsauthfence: writer epoch is zero")
	}
	if !digestPattern.MatchString(cfg.ImageDigest) {
		return errors.New("dsauthfence: image digest must be sha256:<64 lowercase hex>")
	}
	if cfg.KeysetRevision == "" {
		return errors.New("dsauthfence: empty keyset revision")
	}
	if err := validateFeatures(cfg.Features); err != nil {
		return err
	}
	return nil
}

func validateRequiredPolicyForCapability(state RequiredState, service string, writerEpoch uint32, features []string) error {
	if state.RawValue == "" || state.Epoch == 0 {
		return errors.New("dsauthfence: empty required policy state")
	}
	v2Features, known := requiredPolicyV2Features[service]
	if !known {
		return fmt.Errorf("dsauthfence: unknown writer service %q", service)
	}
	v3Features := requiredPolicyV3Features[service]
	switch state.PolicyGeneration {
	case RequiredPolicyGenerationV1:
		if state.Epoch != 1 || state.RawValue != "1" || state.PolicyID != "" || writerEpoch < 1 {
			return errors.New("dsauthfence: invalid baseline required policy state")
		}
		return nil
	case RequiredPolicyGenerationV2:
		if state.Epoch != ProtocolEpochV2 || state.RawValue != RequiredValueV2 ||
			state.PolicyID != RequiredPolicyV2 || writerEpoch != ProtocolEpochV2 {
			return errors.New("dsauthfence: required v2 policy or writer epoch mismatch")
		}
		if equalFeatureSet(features, v2Features) {
			return nil
		}
		// Staging for the one immutable next policy is exact, not a superset
		// exception.  It lets the candidate hub writer register under V2 so a
		// V2→V3 activation can audit it; arbitrary added/removed features fail.
		if service == "hub_allocator" && equalFeatureSet(features, v3Features) {
			return nil
		}
		return fmt.Errorf("dsauthfence: service %s does not advertise exact V2 or staged V3 features", service)
	case RequiredPolicyGenerationV3:
		if state.Epoch != ProtocolEpochV2 || state.RawValue != RequiredValueV3 ||
			state.PolicyID != RequiredPolicyV3 || writerEpoch != ProtocolEpochV2 {
			return errors.New("dsauthfence: required v3 policy or writer epoch mismatch")
		}
		if !equalFeatureSet(features, v3Features) {
			return fmt.Errorf("dsauthfence: service %s does not advertise the exact %s feature policy", service, RequiredPolicyV3)
		}
		return nil
	default:
		return fmt.Errorf("dsauthfence: unsupported required policy generation %d", state.PolicyGeneration)
	}
}

func equalFeatureSet(actual, expected []string) bool {
	if validateFeatures(actual) != nil || validateFeatures(expected) != nil || len(actual) != len(expected) {
		return false
	}
	set := make(map[string]struct{}, len(expected))
	for _, feature := range expected {
		set[feature] = struct{}{}
	}
	for _, feature := range actual {
		if _, ok := set[feature]; !ok {
			return false
		}
	}
	return true
}

func validateFeatures(features []string) error {
	seen := make(map[string]struct{}, len(features))
	for _, feature := range features {
		if feature == "" || strings.TrimSpace(feature) != feature ||
			!capabilityFeaturePattern.MatchString(feature) {
			return fmt.Errorf("dsauthfence: invalid capability feature")
		}
		if _, exists := seen[feature]; exists {
			return fmt.Errorf("dsauthfence: duplicate capability feature")
		}
		seen[feature] = struct{}{}
	}
	return nil
}

func cleanPrefix(prefix string) string       { return strings.TrimSuffix(prefix, "/") + "/" }
func requiredKey(prefix string) string       { return cleanPrefix(prefix) + "required-writer-epoch" }
func capabilityPrefix(prefix string) string  { return cleanPrefix(prefix) + "capabilities/" }
func activationLockKey(prefix string) string { return cleanPrefix(prefix) + "activation-lock" }
func capabilityKey(prefix, service, uid string) string {
	return capabilityPrefix(prefix) + service + "/" + uid
}

// ParseEpoch 只接受规范十进制正整数，避免 "02" 等多种字节表示破坏 CAS。
func ParseEpoch(raw []byte) (uint32, error) {
	s := string(raw)
	if s == "" || s[0] == '0' || strings.TrimSpace(s) != s {
		return 0, fmt.Errorf("invalid epoch %q", s)
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("invalid epoch %q", s)
	}
	return uint32(v), nil
}

// ParseRequiredState deliberately does not accept naked "2". That byte-level
// incompatibility is the rollback fence for old epoch-2 binaries.
func ParseRequiredState(raw []byte) (RequiredState, error) {
	s := string(raw)
	switch s {
	case "1":
		return RequiredState{Epoch: 1, PolicyGeneration: RequiredPolicyGenerationV1, RawValue: s}, nil
	case RequiredValueV2:
		return RequiredState{Epoch: ProtocolEpochV2, PolicyGeneration: RequiredPolicyGenerationV2,
			PolicyID: RequiredPolicyV2, RawValue: s}, nil
	case RequiredValueV3:
		return RequiredState{Epoch: ProtocolEpochV2, PolicyGeneration: RequiredPolicyGenerationV3,
			PolicyID: RequiredPolicyV3, RawValue: s}, nil
	default:
		return RequiredState{}, fmt.Errorf("invalid or unsupported required writer policy %q", s)
	}
}

// RequiredValueForPolicyGeneration returns the immutable raw etcd value for a
// control-plane policy generation.  Generations V2 and V3 deliberately share
// data-plane WriterEpoch=2.
func RequiredValueForPolicyGeneration(generation uint32) (string, error) {
	switch generation {
	case RequiredPolicyGenerationV1:
		return "1", nil
	case RequiredPolicyGenerationV2:
		return RequiredValueV2, nil
	case RequiredPolicyGenerationV3:
		return RequiredValueV3, nil
	default:
		return "", fmt.Errorf("unsupported required policy generation %d", generation)
	}
}

func requiredPolicyIDForGeneration(generation uint32) (string, error) {
	switch generation {
	case RequiredPolicyGenerationV1:
		return "", nil
	case RequiredPolicyGenerationV2:
		return RequiredPolicyV2, nil
	case RequiredPolicyGenerationV3:
		return RequiredPolicyV3, nil
	default:
		return "", fmt.Errorf("unsupported required policy generation %d", generation)
	}
}

func requiredWriterEpochForPolicyGeneration(generation uint32) (uint32, error) {
	switch generation {
	case RequiredPolicyGenerationV1:
		return 1, nil
	case RequiredPolicyGenerationV2, RequiredPolicyGenerationV3:
		return ProtocolEpochV2, nil
	default:
		return 0, fmt.Errorf("unsupported required policy generation %d", generation)
	}
}

func requiredFeaturesForPolicyGeneration(generation uint32) (map[string][]string, error) {
	switch generation {
	case RequiredPolicyGenerationV2:
		return requiredPolicyV2Features, nil
	case RequiredPolicyGenerationV3:
		return requiredPolicyV3Features, nil
	default:
		return nil, fmt.Errorf("unsupported required policy generation %d", generation)
	}
}

// RequiredValueForEpoch returns the sole canonical value for a supported
// transition. It is intentionally not configurable at runtime.
func RequiredValueForEpoch(epoch uint32) (string, error) {
	switch epoch {
	case 1:
		return "1", nil
	case ProtocolEpochV2:
		return RequiredValueV2, nil
	default:
		return "", fmt.Errorf("unsupported required writer epoch %d", epoch)
	}
}

// ValidateActivationPolicy makes the activation tool use the same fixed
// production service/feature policy as runtime Acquire.
func ValidateActivationPolicy(epoch uint32, services map[string]int, features map[string]map[string]struct{}) error {
	if epoch != ProtocolEpochV2 {
		return fmt.Errorf("unsupported activation policy epoch %d", epoch)
	}
	return ValidateActivationPolicyGeneration(RequiredPolicyGenerationV2, services, features)
}

// ValidateActivationPolicyGeneration validates the exact immutable feature
// policy used by an activation candidate.
func ValidateActivationPolicyGeneration(generation uint32, services map[string]int,
	features map[string]map[string]struct{}) error {
	expectedPolicy, err := requiredFeaturesForPolicyGeneration(generation)
	if err != nil {
		return err
	}
	policyID, err := requiredPolicyIDForGeneration(generation)
	if err != nil {
		return err
	}
	if len(services) != len(expectedPolicy) {
		return fmt.Errorf("activation service set does not match %s", policyID)
	}
	for service, expected := range expectedPolicy {
		if services[service] <= 0 {
			return fmt.Errorf("activation service %s missing from %s", service, policyID)
		}
		actualSet := features[service]
		if len(actualSet) != len(expected) {
			return fmt.Errorf("activation feature policy for %s does not match %s", service, policyID)
		}
		for _, feature := range expected {
			if _, ok := actualSet[feature]; !ok {
				return fmt.Errorf("activation feature policy for %s misses %s", service, feature)
			}
		}
	}
	if generation == RequiredPolicyGenerationV3 && services["hub_allocator"] != 1 {
		return fmt.Errorf("activation policy %s requires exactly one hub_allocator writer", policyID)
	}
	for service := range features {
		if _, known := expectedPolicy[service]; !known {
			return fmt.Errorf("activation feature policy contains unknown service %s", service)
		}
	}
	return nil
}

// ExpectedServicesHash 对激活清单做确定性摘要，供 activation record 审计。
func ExpectedServicesHash(services map[string]int) string {
	keys := make([]string, 0, len(services))
	for key := range services {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s=%d\n", key, services[key])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
