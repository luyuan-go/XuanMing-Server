package dsauthfence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

const testDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeLease struct {
	lost chan struct{}
	once sync.Once
}

func newFakeLease() *fakeLease             { return &fakeLease{lost: make(chan struct{})} }
func (l *fakeLease) Lost() <-chan struct{} { return l.lost }
func (l *fakeLease) Close() error          { l.once.Do(func() { close(l.lost) }); return nil }

type fakeBackend struct {
	epoch               uint32
	state               *RequiredState
	found               bool
	err                 error
	watch               chan RequiredEvent
	lease               *fakeLease
	key                 string
	value               []byte
	requiredEpoch       uint32
	requiredValue       string
	requiredModRevision int64

	// 同 Pod 安全接管预检的 staged 残留 capability 与传参记录
	staleValue       []byte
	staleModRevision int64
	staleLeaseID     int64
	staleFound       bool
	staleErr         error
	prevModRevision  int64
	prevLeaseID      int64
	acquireCalls     int
}

func (f *fakeBackend) GetRequired(context.Context, string) (RequiredState, int64, int64, bool, error) {
	if f.err != nil || !f.found {
		return RequiredState{}, 7, 5, f.found, f.err
	}
	if f.state != nil {
		return *f.state, 7, 5, true, nil
	}
	value, err := RequiredValueForEpoch(f.epoch)
	if err != nil {
		return RequiredState{Epoch: f.epoch, RawValue: "unsupported"}, 7, 5, true, nil
	}
	state, err := ParseRequiredState([]byte(value))
	return state, 7, 5, true, err
}
func (f *fakeBackend) GetCapability(context.Context, string) ([]byte, int64, int64, bool, error) {
	if f.staleErr != nil {
		return nil, 0, 0, false, f.staleErr
	}
	if !f.staleFound {
		return nil, 0, 0, false, nil
	}
	return f.staleValue, f.staleModRevision, f.staleLeaseID, true, nil
}
func (f *fakeBackend) AcquireCapability(_ context.Context, key, _, _ string, required string, modRevision int64, value []byte, _ int64, prevModRevision int64, prevLeaseID int64) (Lease, error) {
	f.acquireCalls++
	f.key, f.value = key, value
	f.requiredValue, f.requiredModRevision = required, modRevision
	f.prevModRevision, f.prevLeaseID = prevModRevision, prevLeaseID
	state, _ := ParseRequiredState([]byte(required))
	f.requiredEpoch = state.Epoch
	if f.lease == nil {
		f.lease = newFakeLease()
	}
	return f.lease, nil
}
func (f *fakeBackend) WatchRequired(context.Context, string, int64) <-chan RequiredEvent {
	return f.watch
}
func (f *fakeBackend) Close() error { return nil }

func validConfig() Config {
	return Config{Endpoints: []string{"etcd:2379"}, Service: "hub_allocator", InstanceUID: "pod-uid", ImageDigest: testDigest, KeysetRevision: "r1", WriterEpoch: 2,
		Features: []string{"hub-reservation-ledger-v1", "hub-heartbeat-capacity-v1", "hub-owner-cleanup-v1", "hub-physical-eviction-v1"}}
}

func validV3Config() Config {
	cfg := validConfig()
	cfg.Features = append(append([]string(nil), cfg.Features...), "hub-successor-lease-v1")
	return cfg
}

func TestStartRequiresLinearRequiredKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend *fakeBackend
	}{
		{name: "missing", backend: &fakeBackend{watch: make(chan RequiredEvent)}},
		{name: "read_failure", backend: &fakeBackend{err: errors.New("down"), watch: make(chan RequiredEvent)}},
		{name: "future", backend: &fakeBackend{epoch: 3, found: true, watch: make(chan RequiredEvent)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Start(context.Background(), tc.backend, validConfig()); err == nil {
				t.Fatal("expected fail-closed error")
			}
		})
	}
}

func TestStartFencesCapabilityWithExactRequiredRevision(t *testing.T) {
	backend := &fakeBackend{epoch: 1, found: true, watch: make(chan RequiredEvent)}
	holder, err := Start(context.Background(), backend, validConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if backend.requiredEpoch != 1 || backend.requiredValue != "1" || backend.requiredModRevision != 5 {
		t.Fatalf("capability compare value=%q epoch=%d mod_revision=%d", backend.requiredValue, backend.requiredEpoch, backend.requiredModRevision)
	}
}

// staleCapability 构造残留 capability JSON(默认与 validConfig 同身份,可用 mutate 改字段)。
func staleCapability(t *testing.T, cfg Config, mutate func(*Capability)) []byte {
	t.Helper()
	prev := Capability{
		Service: cfg.Service, InstanceUID: cfg.InstanceUID, WriterEpoch: cfg.WriterEpoch,
		ImageDigest: cfg.ImageDigest, KeysetRevision: cfg.KeysetRevision,
	}
	if mutate != nil {
		mutate(&prev)
	}
	raw, err := json.Marshal(prev)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestStartReclaimsSamePodStaleCapability:同 Pod 上一进程 fatal 崩溃残留的同身份
// capability 必须被安全接管(ModRevision CAS + 旧租约 revoke),不再等 TTL 自然过期
// (2026-07-21 allocator 崩溃后重启被 duplicate key 拒、恢复晚于 20s 授权租约)。
func TestStartReclaimsSamePodStaleCapability(t *testing.T) {
	cfg := validConfig()
	backend := &fakeBackend{
		epoch: 1, found: true, watch: make(chan RequiredEvent),
		staleFound: true, staleValue: staleCapability(t, cfg, nil),
		staleModRevision: 42, staleLeaseID: 99,
	}
	holder, err := Start(context.Background(), backend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if backend.prevModRevision != 42 || backend.prevLeaseID != 99 {
		t.Fatalf("takeover CAS prev_mod_revision=%d prev_lease_id=%d, want 42/99",
			backend.prevModRevision, backend.prevLeaseID)
	}
	if !holder.Reclaimed() {
		t.Fatal("holder should report reclaimed capability")
	}
}

// TestStartRefusesForeignStaleCapability:残留 capability 任一身份字段不一致
// (异身份写入 / 镜像已换 / writer epoch / keyset 漂移 / 值不可解析)必须拒绝接管
// fail-closed,且绝不发起注册事务。
func TestStartRefusesForeignStaleCapability(t *testing.T) {
	cfg := validConfig()
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "unparsable", value: []byte("{not json")},
		{name: "instance_uid", value: staleCapability(t, cfg, func(c *Capability) { c.InstanceUID = "other-pod" })},
		{name: "image_digest", value: staleCapability(t, cfg, func(c *Capability) {
			c.ImageDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		})},
		{name: "writer_epoch", value: staleCapability(t, cfg, func(c *Capability) { c.WriterEpoch = 1 })},
		{name: "keyset_revision", value: staleCapability(t, cfg, func(c *Capability) { c.KeysetRevision = "r2" })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeBackend{
				epoch: 1, found: true, watch: make(chan RequiredEvent),
				staleFound: true, staleValue: tc.value, staleModRevision: 42, staleLeaseID: 99,
			}
			if _, err := Start(context.Background(), backend, cfg); err == nil {
				t.Fatal("expected fail-closed takeover refusal")
			}
			if backend.acquireCalls != 0 {
				t.Fatalf("acquire must not run after takeover refusal, got %d calls", backend.acquireCalls)
			}
		})
	}
}

// TestStartFreshAcquireUsesCreateRevisionGuard:无残留 key 时保持原「key 必须不存在」
// 语义(prevModRevision/prevLeaseID 均为 0)。
func TestStartFreshAcquireUsesCreateRevisionGuard(t *testing.T) {
	backend := &fakeBackend{epoch: 1, found: true, watch: make(chan RequiredEvent)}
	holder, err := Start(context.Background(), backend, validConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if backend.prevModRevision != 0 || backend.prevLeaseID != 0 {
		t.Fatalf("fresh acquire prev=%d/%d, want 0/0", backend.prevModRevision, backend.prevLeaseID)
	}
	if holder.Reclaimed() {
		t.Fatal("fresh acquire must not report reclaimed")
	}
}

func TestRuntimeIdentityHasNoFallback(t *testing.T) {
	t.Setenv(EnvPodUID, "")
	t.Setenv(EnvImageDigest, "")
	_, err := AcquireRuntime(context.Background(), RuntimeConfig{
		Endpoints: []string{"etcd:2379"}, Service: "hub_allocator", KeysetRevision: "r1",
	})
	if err == nil || !strings.Contains(err.Error(), "instance uid") {
		t.Fatalf("expected missing downward-api identity error, got %v", err)
	}
}

func TestHolderMonotonicAndFailClosed(t *testing.T) {
	watch := make(chan RequiredEvent, 3)
	backend := &fakeBackend{epoch: 1, found: true, watch: watch}
	holder, err := Start(context.Background(), backend, validConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if holder.RequiredEpoch() != 1 {
		t.Fatalf("required=%d", holder.RequiredEpoch())
	}
	watch <- RequiredEvent{State: RequiredState{Epoch: 2, PolicyGeneration: RequiredPolicyGenerationV2, PolicyID: RequiredPolicyV2, RawValue: RequiredValueV2}, Revision: 8}
	select {
	case <-holder.Lost():
	case <-time.After(time.Second):
		t.Fatal("forward policy change did not force capability reacquisition")
	}
	if holder.RequiredEpoch() != 2 || holder.RequiredPolicyGeneration() != RequiredPolicyGenerationV2 {
		t.Fatalf("holder did not publish observed forward policy before fencing: epoch=%d generation=%d",
			holder.RequiredEpoch(), holder.RequiredPolicyGeneration())
	}
}

// TestHolderLostReasonIdentifiesBranch 钉住失效原因的分支可辨识性。
//
// 动因(2026-07-29 ds_allocator 保护性退出取证):五个触发点此前在最终日志里完全
// 同形,只能看到 ds_auth_fence_lost,无法区分「租约 keepalive 断」与「required watch
// 断/回退/推进」。事故当时 etcd 正在几十秒级 fdatasync 卡顿,两类都可能,取证到此为止。
// 本测试保证每个分支各自落到唯一常量上,回归后不再被合并成同一个不可分辨信号。
func TestHolderLostReasonIdentifiesBranch(t *testing.T) {
	// 初始 required modRevision 由 fakeBackend.GetRequired 固定返回 5,故 Revision<=5
	// 即构成回退;Revision>5 且 policy generation 前进即构成正常推进。
	validV2 := RequiredState{Epoch: 2, PolicyGeneration: RequiredPolicyGenerationV2,
		PolicyID: RequiredPolicyV2, RawValue: RequiredValueV2}
	for name, tc := range map[string]struct {
		trigger func(watch chan RequiredEvent, lease *fakeLease)
		want    string
	}{
		"lease_keepalive_ended": {
			trigger: func(_ chan RequiredEvent, lease *fakeLease) { lease.Close() },
			want:    LostReasonLeaseKeepaliveEnded,
		},
		"watch_error": {
			trigger: func(watch chan RequiredEvent, _ *fakeLease) {
				watch <- RequiredEvent{Err: errors.New("etcdserver: mvcc: required revision has been compacted")}
			},
			want: LostReasonRequiredWatchError,
		},
		"required_deleted": {
			trigger: func(watch chan RequiredEvent, _ *fakeLease) {
				watch <- RequiredEvent{Deleted: true, Revision: 9}
			},
			want: LostReasonRequiredDeleted,
		},
		"required_regressed": {
			trigger: func(watch chan RequiredEvent, _ *fakeLease) {
				watch <- RequiredEvent{State: validV2, Revision: 3}
			},
			want: LostReasonRequiredRegressed,
		},
		"required_advanced": {
			trigger: func(watch chan RequiredEvent, _ *fakeLease) {
				watch <- RequiredEvent{State: validV2, Revision: 9}
			},
			want: LostReasonRequiredAdvanced,
		},
		"watch_closed": {
			trigger: func(watch chan RequiredEvent, _ *fakeLease) { close(watch) },
			want:    LostReasonRequiredWatchClosed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			watch := make(chan RequiredEvent, 1)
			lease := newFakeLease()
			backend := &fakeBackend{epoch: 1, found: true, watch: watch, lease: lease}
			holder, err := Start(context.Background(), backend, validConfig())
			if err != nil {
				t.Fatal(err)
			}
			if got := holder.LostReason(); got != "" {
				t.Fatalf("reason before loss = %q, want empty", got)
			}
			tc.trigger(watch, lease)
			select {
			case <-holder.Lost():
			case <-time.After(time.Second):
				t.Fatal("branch did not fence")
			}
			if got := holder.LostReason(); got != tc.want {
				t.Fatalf("LostReason = %q, want %q", got, tc.want)
			}
			_ = holder.Close()
		})
	}
}

func TestLeaseLossAndWatchCloseFence(t *testing.T) {
	for _, leaseLoss := range []bool{true, false} {
		watch := make(chan RequiredEvent)
		lease := newFakeLease()
		backend := &fakeBackend{epoch: 1, found: true, watch: watch, lease: lease}
		holder, err := Start(context.Background(), backend, validConfig())
		if err != nil {
			t.Fatal(err)
		}
		if leaseLoss {
			lease.Close()
		} else {
			close(watch)
		}
		select {
		case <-holder.Lost():
		case <-time.After(time.Second):
			t.Fatal("loss did not fence")
		}
		_ = holder.Close()
	}
}

func TestCapabilityAuditExactSet(t *testing.T) {
	cap := Capability{Service: "hub_allocator", InstanceUID: "uid", WriterEpoch: 2, ImageDigest: testDigest, KeysetRevision: "r1", EtcdIdentityRevision: "r7", Features: []string{"hub-reservation-ledger-v1", "hub-heartbeat-capacity-v1"}}
	policy := AuditPolicy{RequiredServices: map[string]int{"hub_allocator": 1}, TargetEpoch: 2, KeysetRevision: "r1", AllowedDigests: map[string]struct{}{testDigest: {}}, ExpectedDigests: map[string]string{"hub_allocator": testDigest}}
	policy.RequiredInstances = map[string]map[string]struct{}{"hub_allocator": {"uid": {}}}
	policy.RequiredFeatures = map[string]map[string]struct{}{"hub_allocator": {"hub-reservation-ledger-v1": {}, "hub-heartbeat-capacity-v1": {}}}
	policy.EtcdIdentityRevision = "r7"
	if got := AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/pandora/ds-auth/capabilities/hub_allocator/uid"}}, policy); len(got) != 0 {
		t.Fatalf("unexpected findings: %v", got)
	}
	cap.WriterEpoch = 1
	policy.RequiredServices["hub_allocator"] = 2
	got := AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/pandora/ds-auth/capabilities/hub_allocator/uid"}}, policy)
	if strings.Join(got, " ") == "" {
		t.Fatal("expected findings")
	}
	cap.WriterEpoch = 2
	cap.Features = append(cap.Features, "not canonical")
	policy.RequiredServices["hub_allocator"] = 1
	got = AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/pandora/ds-auth/capabilities/hub_allocator/uid"}}, policy)
	if !strings.Contains(strings.Join(got, " "), "非 canonical") {
		t.Fatalf("malformed live capability feature accepted: %v", got)
	}
	cap.Features = []string{"hub-reservation-ledger-v1", "hub-heartbeat-capacity-v1", "unexpected-feature-v1"}
	got = AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/pandora/ds-auth/capabilities/hub_allocator/uid"}}, policy)
	if !strings.Contains(strings.Join(got, " "), "未批准") {
		t.Fatalf("extra live capability feature accepted: %v", got)
	}
	cap.Features = []string{"hub-reservation-ledger-v1", "hub-heartbeat-capacity-v1"}
	got = AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/wrong/capabilities/hub_allocator/uid"}}, policy)
	if !strings.Contains(strings.Join(got, " "), "key 与 payload") {
		t.Fatalf("capability outside the configured prefix accepted: %v", got)
	}
	cap.ImageDigest = "sha256:" + strings.Repeat("b", 64)
	policy.AllowedDigests[cap.ImageDigest] = struct{}{}
	got = AuditCapabilities([]LiveCapability{{Capability: cap, LeaseID: 1, Key: "/pandora/ds-auth/capabilities/hub_allocator/uid"}}, policy)
	if !strings.Contains(strings.Join(got, " "), "service expected") {
		t.Fatalf("digest from another allowed service accepted: %v", got)
	}
}

func TestCapabilityFeaturesAreValidatedWithoutMutatingCaller(t *testing.T) {
	features := []string{"z-feature-v1", "a-feature-v1"}
	cfg := validConfig()
	cfg.Features = features
	backend := &fakeBackend{epoch: 1, found: true, watch: make(chan RequiredEvent)}
	holder, err := Start(context.Background(), backend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if got := strings.Join(features, ","); got != "z-feature-v1,a-feature-v1" {
		t.Fatalf("caller feature slice mutated: %s", got)
	}
	var capability Capability
	if err := json.Unmarshal(backend.value, &capability); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(capability.Features, ","); got != "z-feature-v1,a-feature-v1" {
		t.Fatalf("capability features=%s", got)
	}

	for _, invalid := range [][]string{{"bad feature"}, {"ok-feature-v1", "ok-feature-v1"}, {"UPPER-v1"}} {
		cfg := validConfig()
		cfg.Features = invalid
		if _, err := Start(context.Background(), &fakeBackend{epoch: 1, found: true, watch: make(chan RequiredEvent)}, cfg); err == nil {
			t.Fatalf("accepted invalid features %v", invalid)
		}
	}
}

func TestParseEpochAndExpectedServices(t *testing.T) {
	for _, raw := range []string{"", "0", "02", " 2", "2 ", "-1"} {
		if _, err := ParseEpoch([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	if epoch, err := ParseEpoch([]byte("2")); err != nil || epoch != 2 {
		t.Fatalf("epoch=%d err=%v", epoch, err)
	}
	services, err := ParseExpectedServices("hub_allocator=2,player_locator=3")
	if err != nil || services["player_locator"] != 3 {
		t.Fatalf("services=%v err=%v", services, err)
	}
	instances, err := ParseExpectedInstances("hub_allocator=uid-a|uid-b,player_locator=uid-c")
	if err != nil || len(instances["hub_allocator"]) != 2 {
		t.Fatalf("instances=%v err=%v", instances, err)
	}
	features, err := ParseRequiredFeatures("hub_allocator=hub-reservation-ledger-v1|hub-heartbeat-capacity-v1,battle_result=battle-terminal-outbox-v1")
	if err != nil || len(features["hub_allocator"]) != 2 {
		t.Fatalf("features=%v err=%v", features, err)
	}
	for _, raw := range []string{
		"hub_allocator=bad feature",
		"hub_allocator=hub-reservation-ledger-v1|hub-reservation-ledger-v1",
		"hub_allocator=x,hub_allocator=y",
	} {
		if _, err := ParseRequiredFeatures(raw); err == nil {
			t.Fatalf("accepted invalid required features %q", raw)
		}
	}
	digests, err := ParseExpectedDigests("hub_allocator=" + testDigest + ",player_locator=" + testDigest)
	if err != nil || digests["hub_allocator"] != testDigest {
		t.Fatalf("digests=%v err=%v", digests, err)
	}
	for _, raw := range []string{"", "hub_allocator=latest", "hub_allocator=" + testDigest + ",hub_allocator=" + testDigest, "bad/name=" + testDigest} {
		if _, err := ParseExpectedDigests(raw); err == nil {
			t.Fatalf("accepted invalid expected digest %q", raw)
		}
	}
}

func TestVersionedRequiredPolicyMechanicallyFencesOldEpoch2Binary(t *testing.T) {
	if _, err := ParseEpoch([]byte(RequiredValueV2)); err == nil {
		t.Fatal("old numeric parser accepted the versioned v2 policy value")
	}
	state, err := ParseRequiredState([]byte(RequiredValueV2))
	if err != nil || state.Epoch != ProtocolEpochV2 || state.PolicyGeneration != RequiredPolicyGenerationV2 || state.PolicyID != RequiredPolicyV2 || state.RawValue != RequiredValueV2 {
		t.Fatalf("versioned state=%+v err=%v", state, err)
	}
	v3, err := ParseRequiredState([]byte(RequiredValueV3))
	if err != nil || v3.Epoch != ProtocolEpochV2 || v3.PolicyGeneration != RequiredPolicyGenerationV3 ||
		v3.PolicyID != RequiredPolicyV3 || v3.RawValue != RequiredValueV3 {
		t.Fatalf("versioned V3 state=%+v err=%v", v3, err)
	}
	for _, raw := range []string{"2", "2@wrong-policy", "1@" + RequiredPolicyV2, RequiredValueV2 + " ", "3@future"} {
		if _, err := ParseRequiredState([]byte(raw)); err == nil {
			t.Fatalf("new parser accepted non-canonical required policy %q", raw)
		}
	}
	if got, err := RequiredValueForEpoch(2); err != nil || got != RequiredValueV2 {
		t.Fatalf("RequiredValueForEpoch(2)=%q err=%v", got, err)
	}
}

func TestStartTargetPolicyRequiresExactServiceFeaturesAndRawCASValue(t *testing.T) {
	state := RequiredState{Epoch: 2, PolicyGeneration: RequiredPolicyGenerationV2, PolicyID: RequiredPolicyV2, RawValue: RequiredValueV2}
	backend := &fakeBackend{state: &state, found: true, watch: make(chan RequiredEvent)}
	holder, err := Start(context.Background(), backend, validConfig())
	if err != nil {
		t.Fatalf("exact target policy rejected: %v", err)
	}
	defer holder.Close()
	if backend.requiredValue != RequiredValueV2 {
		t.Fatalf("registration compared %q, want %q", backend.requiredValue, RequiredValueV2)
	}

	for name, mutate := range map[string]func(*Config){
		"missing feature": func(cfg *Config) { cfg.Features = cfg.Features[:len(cfg.Features)-1] },
		"extra feature":   func(cfg *Config) { cfg.Features = append(cfg.Features, "unexpected-feature-v1") },
		"unknown service": func(cfg *Config) { cfg.Service = "unknown_writer" },
		"future writer":   func(cfg *Config) { cfg.WriterEpoch = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			copyState := state
			if _, err := Start(context.Background(), &fakeBackend{state: &copyState, found: true, watch: make(chan RequiredEvent)}, cfg); err == nil {
				t.Fatal("invalid target capability acquired")
			}
		})
	}
}

func TestV2AllowsOnlyExactHubV2OrStagedV3AndV3RejectsOldHub(t *testing.T) {
	v2, _ := ParseRequiredState([]byte(RequiredValueV2))
	for name, cfg := range map[string]Config{"exact-v2": validConfig(), "exact-staged-v3": validV3Config()} {
		t.Run(name, func(t *testing.T) {
			backend := &fakeBackend{state: &v2, found: true, watch: make(chan RequiredEvent)}
			holder, err := Start(context.Background(), backend, cfg)
			if err != nil {
				t.Fatalf("canonical candidate rejected: %v", err)
			}
			defer holder.Close()
			var capability Capability
			if err := json.Unmarshal(backend.value, &capability); err != nil {
				t.Fatal(err)
			}
			if capability.SupportedPolicyGeneration != RequiredPolicyGenerationV3 ||
				capability.SupportedPolicyID != RequiredPolicyV3 {
				t.Fatalf("compiled policy identity missing from capability: %+v", capability)
			}
			if capability.AcquiredPolicyGeneration != RequiredPolicyGenerationV2 ||
				capability.AcquiredPolicyID != RequiredPolicyV2 {
				t.Fatalf("actual required policy identity missing from capability: %+v", capability)
			}
		})
	}
	arbitrary := validConfig()
	arbitrary.Features = append(arbitrary.Features, "unrelated-feature-v1")
	if _, err := Start(context.Background(), &fakeBackend{state: &v2, found: true, watch: make(chan RequiredEvent)}, arbitrary); err == nil {
		t.Fatal("arbitrary V2 feature superset was accepted")
	}

	v3, _ := ParseRequiredState([]byte(RequiredValueV3))
	if _, err := Start(context.Background(), &fakeBackend{state: &v3, found: true, watch: make(chan RequiredEvent)}, validConfig()); err == nil {
		t.Fatal("old Hub feature set reacquired under V3")
	}
	holder, err := Start(context.Background(), &fakeBackend{state: &v3, found: true, watch: make(chan RequiredEvent)}, validV3Config())
	if err != nil {
		t.Fatalf("exact V3 Hub rejected: %v", err)
	}
	_ = holder.Close()
}

func TestV3AuditRejectsOldBinaryEvenWhenFeaturesAndDigestMatch(t *testing.T) {
	features, err := ParseRequiredFeatures(
		"hub_allocator=hub-reservation-ledger-v1|hub-heartbeat-capacity-v1|hub-owner-cleanup-v1|hub-physical-eviction-v1|hub-successor-lease-v1")
	if err != nil {
		t.Fatal(err)
	}
	capability := Capability{Service: "hub_allocator", InstanceUID: "uid", WriterEpoch: 2,
		ImageDigest: testDigest, KeysetRevision: "r1", Features: validV3Config().Features}
	policy := AuditPolicy{RequiredServices: map[string]int{"hub_allocator": 1},
		TargetEpoch: 2, TargetPolicyGeneration: RequiredPolicyGenerationV3,
		ExpectedAcquiredPolicyGeneration: RequiredPolicyGenerationV3,
		KeysetRevision:                   "r1", AllowedDigests: map[string]struct{}{testDigest: {}},
		ExpectedDigests: map[string]string{"hub_allocator": testDigest}, RequiredFeatures: features}
	live := LiveCapability{Capability: capability, LeaseID: 1,
		Key: capabilityKey(DefaultPrefix, "hub_allocator", "uid")}
	if findings := AuditCapabilities([]LiveCapability{live}, policy); !strings.Contains(strings.Join(findings, " "), "supported_policy") {
		t.Fatalf("legacy capability without compiled policy identity passed V3 audit: %v", findings)
	}
	live.Capability.SupportedPolicyGeneration = RequiredPolicyGenerationV3
	live.Capability.SupportedPolicyID = RequiredPolicyV3
	live.Capability.AcquiredPolicyGeneration = RequiredPolicyGenerationV3
	live.Capability.AcquiredPolicyID = RequiredPolicyV3
	if findings := AuditCapabilities([]LiveCapability{live}, policy); len(findings) != 0 {
		t.Fatalf("exact compiled V3 capability rejected: %v", findings)
	}
}

func TestTargetPolicyWatchDeletionTamperAndSameEpochRewriteFence(t *testing.T) {
	tests := map[string]RequiredEvent{
		"deletion":        {Deleted: true, Revision: 8},
		"wrong policy":    {State: RequiredState{Epoch: 2, PolicyID: "wrong", RawValue: "2@wrong"}, Revision: 8},
		"same epoch edit": {State: RequiredState{Epoch: 1, RawValue: "1"}, Revision: 8},
	}
	for name, event := range tests {
		t.Run(name, func(t *testing.T) {
			watch := make(chan RequiredEvent, 1)
			backend := &fakeBackend{epoch: 1, found: true, watch: watch}
			holder, err := Start(context.Background(), backend, validConfig())
			if err != nil {
				t.Fatal(err)
			}
			watch <- event
			select {
			case <-holder.Lost():
			case <-time.After(time.Second):
				t.Fatal("required policy tamper did not fence")
			}
			_ = holder.Close()
		})
	}
}

func TestActivationPolicyIsFixedAndExact(t *testing.T) {
	services := map[string]int{
		"login": 1, "player_locator": 1, "ds_allocator": 1, "hub_allocator": 1, "battle_result": 1,
	}
	features, err := ParseRequiredFeatures(
		"hub_allocator=hub-reservation-ledger-v1|hub-heartbeat-capacity-v1|hub-owner-cleanup-v1|hub-physical-eviction-v1," +
			"ds_allocator=battle-release-expected-tuple-v1|battle-storage-pod-uid-write-invariant-v1," +
			"battle_result=battle-terminal-outbox-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateActivationPolicy(2, services, features); err != nil {
		t.Fatalf("canonical activation policy rejected: %v", err)
	}
	delete(features["ds_allocator"], "battle-storage-pod-uid-write-invariant-v1")
	if err := ValidateActivationPolicy(2, services, features); err == nil {
		t.Fatal("activation policy missing pod_uid invariant was accepted")
	}
	delete(services, "battle_result")
	if err := ValidateActivationPolicy(2, services, features); err == nil {
		t.Fatal("activation policy missing production writer was accepted")
	}
}

func TestV3ActivationRequiresSingleHubWriter(t *testing.T) {
	services := map[string]int{
		"login": 1, "player_locator": 1, "ds_allocator": 1, "hub_allocator": 1, "battle_result": 1,
	}
	features := make(map[string]map[string]struct{}, len(requiredPolicyV3Features))
	for service, list := range requiredPolicyV3Features {
		features[service] = make(map[string]struct{}, len(list))
		for _, feature := range list {
			features[service][feature] = struct{}{}
		}
	}
	if err := ValidateActivationPolicyGeneration(RequiredPolicyGenerationV3, services, features); err != nil {
		t.Fatalf("canonical single-Hub V3 policy rejected: %v", err)
	}
	services["hub_allocator"] = 2
	if err := ValidateActivationPolicyGeneration(RequiredPolicyGenerationV3, services, features); err == nil {
		t.Fatal("V3 activation accepted two Hub writers despite the Recreate/single-writer contract")
	}
}
