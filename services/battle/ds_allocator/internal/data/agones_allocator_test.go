// agones_allocator_test.go — AgonesGameServerAllocator 单测(W4 ⑫)。
//
// 用 httptest 模拟 k8s apiserver,不连真集群:
//   - Allocate: 校验请求方法/路径/body selector + 解析 Allocated status → podName/addr
//   - Allocate: status=UnAllocated → ErrDSNoAvailable
//   - Allocate: apiserver 5xx → ErrDSAllocationFailed
//   - Release: DELETE 正确路径 → nil;404 → nil(幂等)
package data

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

func newTestAllocator(t *testing.T, serverURL string) *AgonesGameServerAllocator {
	t.Helper()
	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		Enabled:   true,
		APIServer: serverURL,
		Namespace: "pandora",
		FleetName: "battle-fleet",
		TokenPath: "-", // 不带 token
	})
	if err != nil {
		t.Fatalf("NewAgonesGameServerAllocator: %v", err)
	}
	return a
}

func writeOwnedPod(w http.ResponseWriter, name, podUID, gameServerUID string) {
	_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
		"name": name, "uid": podUID, "resourceVersion": "201",
		"ownerReferences": []map[string]any{{
			"apiVersion": "agones.dev/v1", "kind": "GameServer", "name": name,
			"uid": gameServerUID, "controller": true,
		}},
	}})
}

func TestNewAgonesGameServerAllocator_RequiresFleet(t *testing.T) {
	if _, err := NewAgonesGameServerAllocator(conf.AgonesConf{Enabled: true}); err == nil {
		t.Fatal("expected error when fleet_name empty, got nil")
	}
}

func TestAgonesAllocate_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s want POST", r.Method)
		}
		wantPath := "/apis/allocation.agones.dev/v1/namespaces/pandora/gameserverallocations"
		if r.URL.Path != wantPath {
			t.Errorf("path: got %s want %s", r.URL.Path, wantPath)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "agones.dev/fleet") || !strings.Contains(string(body), "battle-fleet") {
			t.Errorf("request body missing fleet selector: %s", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"state":          "Allocated",
				"gameServerName": "battle-fleet-abc12",
				"address":        "10.0.0.7",
				"ports":          []map[string]any{{"name": "default", "port": 7777}},
			},
		})
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	pod, addr, track, err := a.Allocate(context.Background(), 12345, 2, "moba_5v5", "stable")
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if pod != "battle-fleet-abc12" {
		t.Errorf("pod: got %q want battle-fleet-abc12", pod)
	}
	if addr != "10.0.0.7:7777" {
		t.Errorf("addr: got %q want 10.0.0.7:7777", addr)
	}
	if track != "stable" {
		t.Errorf("track: got %q want stable", track)
	}
}

// TestAgonesAllocate_DSTokenAnnotation:注入 dsTokenIssuer 后,分配请求的 metadata.annotations
// 必须携带 pandora.dev/ds-token(DS 回调服务令牌下发通道,审核 P1 #1);未注入时不出现该字段。
func TestAgonesAllocate_DSTokenAnnotation(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"state":          "Allocated",
				"gameServerName": "battle-fleet-abc12",
				"address":        "10.0.0.7",
				"ports":          []map[string]any{{"name": "default", "port": 7777}},
			},
		})
	}))
	defer srv.Close()

	// 未注入 issuer:不带 annotations。
	a := newTestAllocator(t, srv.URL)
	if _, _, _, err := a.Allocate(context.Background(), 42, 1, "moba", "stable"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if strings.Contains(string(gotBody), "pandora.dev/ds-token") {
		t.Errorf("annotations must be absent without issuer: %s", gotBody)
	}

	// 注入 issuer:annotation 带上令牌。
	a.SetDSTokenIssuer(func(matchID uint64) (string, error) { return "tok-for-42", nil }, false)
	if _, _, _, err := a.Allocate(context.Background(), 42, 1, "moba", "stable"); err != nil {
		t.Fatalf("Allocate with issuer: %v", err)
	}
	var req struct {
		Spec struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if got := req.Spec.Metadata.Annotations["pandora.dev/ds-token"]; got != "tok-for-42" {
		t.Errorf("ds-token annotation: got %q want tok-for-42", got)
	}

	// issuer 报错 + off/permissive(required=false):降级为无令牌分配,不阻断。
	a.SetDSTokenIssuer(func(matchID uint64) (string, error) { return "", context.DeadlineExceeded }, false)
	if _, _, _, err := a.Allocate(context.Background(), 42, 1, "moba", "stable"); err != nil {
		t.Fatalf("Allocate with failing issuer must not fail: %v", err)
	}
	if strings.Contains(string(gotBody), "pandora.dev/ds-token") {
		t.Errorf("annotations must be absent when issuer fails: %s", gotBody)
	}

	// issuer 报错 + enforce(required=true):fail-closed,Allocate 返回分配失败。
	a.SetDSTokenIssuer(func(matchID uint64) (string, error) { return "", context.DeadlineExceeded }, true)
	if _, _, _, err := a.Allocate(context.Background(), 42, 1, "moba", "stable"); err == nil {
		t.Fatal("Allocate under enforce with failing issuer must fail")
	} else if got := errcode.As(err); got != errcode.ErrDSAllocationFailed {
		t.Errorf("enforce sign-fail code: got %d want ErrDSAllocationFailed", got)
	}
}

func TestAllocateAuthoritative_POSTWithoutTokenThenStrictGETIdentity(t *testing.T) {
	var issuerCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), "pandora.dev/ds-token") {
				t.Errorf("Model B GSA POST must not contain token: %s", body)
			}
			if !strings.Contains(string(body), "pandora.dev/allocation-id") {
				t.Errorf("GSA POST missing persistent allocation-id label: %s", body)
			}
			if !strings.Contains(string(body), `"pandora.dev/roster":"2,9"`) ||
				!strings.Contains(string(body), `"pandora.dev/combat-factions":"2=4,9=4"`) ||
				!strings.Contains(string(body), `"pandora.dev/release-track":"stable"`) {
				t.Errorf("GSA POST missing canonical roster/combat-factions/release metadata: %s", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
				"state": "Allocated", "gameServerName": "battle-fleet-auth1",
				"address": "10.0.0.8", "ports": []map[string]any{{"port": 7777}},
			}})
		case http.MethodGet:
			if strings.Contains(r.URL.Path, "/pods/") {
				writeOwnedPod(w, "battle-fleet-auth1", "pod-uid-auth1", "uid-auth1")
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
				"name": "battle-fleet-auth1", "uid": "uid-auth1", "resourceVersion": "101",
				"labels": map[string]string{
					"pandora.dev/match-id": "42", "pandora.dev/allocation-id": "11111111-1111-4111-8111-111111111111",
					"pandora.dev/release-track": "stable",
				},
				"annotations": map[string]string{
					"pandora.dev/allocation-id": "11111111-1111-4111-8111-111111111111",
					"pandora.dev/roster":        "2,9", "pandora.dev/combat-factions": "2=4,9=4",
					"pandora.dev/release-track": "stable",
				},
			}})
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	a.SetDSTokenIssuer(func(uint64) (string, error) {
		issuerCalled.Store(true)
		return "must-not-be-used", nil
	}, true)
	const allocationID = "11111111-1111-4111-8111-111111111111"
	got, err := a.AllocateAuthoritative(context.Background(), 42, allocationID,
		[]uint64{9, 2, 9}, map[uint64]uint32{2: 4, 9: 4}, 1, "ranked", "stable")
	if err != nil {
		t.Fatalf("AllocateAuthoritative: %v", err)
	}
	if issuerCalled.Load() {
		t.Fatal("Model B signed token before selected GameServer UID was known")
	}
	if got.PodName != "battle-fleet-auth1" || got.Addr != "10.0.0.8:7777" ||
		got.InstanceUID != "uid-auth1" || got.PodUID != "pod-uid-auth1" ||
		got.ResourceVersion != "101" || got.ReleaseTrack != "stable" {
		t.Fatalf("authoritative allocation mismatch: %+v", got)
	}
}

func TestAllocateAuthoritative_StrictGETRejectsMissingCombatFactions(t *testing.T) {
	const allocationID = "12121212-1212-4212-8212-121212121212"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
				"state": "Allocated", "gameServerName": "battle-faction-missing",
				"address": "10.0.0.8", "ports": []map[string]any{{"port": 7777}},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
			"name": "battle-faction-missing", "uid": "uid-faction-missing", "resourceVersion": "101",
			"labels": map[string]string{
				"pandora.dev/match-id": "42", battleAllocationMetadataKey: allocationID,
				releaseTrackMetadataKey: "stable",
			},
			"annotations": map[string]string{
				battleAllocationMetadataKey: allocationID, battleRosterAnnotationKey: "1",
				releaseTrackMetadataKey: "stable",
			},
		}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if _, err := a.AllocateAuthoritative(context.Background(), 42, allocationID,
		[]uint64{1}, map[uint64]uint32{1: 2}, 1, "ranked", "stable"); err == nil {
		t.Fatal("strict GET accepted missing combat-factions annotation")
	}
}

func TestAuthoritativeAllocationRejectsVersion4WithNonRFC4122Variant(t *testing.T) {
	a := &AgonesGameServerAllocator{}
	const nonRFCVariant = "11111111-1111-4111-0111-111111111111"
	if _, err := a.AllocateAuthoritative(context.Background(), 42, nonRFCVariant,
		[]uint64{1}, nil, 1, "ranked", "stable"); err == nil {
		t.Fatal("AllocateAuthoritative accepted UUIDv4 with non-RFC4122 variant")
	}
	if _, found, err := a.ResolveAllocationByID(context.Background(), 42, nonRFCVariant,
		[]uint64{1}, nil, 1, "ranked"); err == nil || found {
		t.Fatalf("ResolveAllocationByID accepted UUIDv4 with non-RFC4122 variant: found=%v err=%v", found, err)
	}
	if _, err := a.ResolveExpectedPodUID(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "pod-1", InstanceUID: "uid-1", AllocationID: nonRFCVariant,
	}); err == nil {
		t.Fatal("ResolveExpectedPodUID accepted UUIDv4 with non-RFC4122 variant")
	}
}

func TestResolveExpectedPodUIDRequiresExactGameServerAndOwnedPod(t *testing.T) {
	const allocationID = "11111111-1111-4111-8111-111111111111"
	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("preflight side effect method=%s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		gets.Add(1)
		if strings.Contains(r.URL.Path, "/pods/") {
			writeOwnedPod(w, "battle-legacy-1", "pod-uid-legacy-1", "gs-uid-legacy-1")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
			"name": "battle-legacy-1", "uid": "gs-uid-legacy-1", "resourceVersion": "101",
			"labels":      map[string]string{"pandora.dev/allocation-id": allocationID},
			"annotations": map[string]string{"pandora.dev/allocation-id": allocationID},
		}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	podUID, err := a.ResolveExpectedPodUID(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-legacy-1", InstanceUID: "gs-uid-legacy-1", AllocationID: allocationID,
	})
	if err != nil || podUID != "pod-uid-legacy-1" {
		t.Fatalf("exact pod UID preflight: pod_uid=%q err=%v", podUID, err)
	}
	if gets.Load() != 2 {
		t.Fatalf("exact preflight GET calls=%d want 2", gets.Load())
	}
}

func TestResolveExpectedPodUIDRejectsMissingOrRecreatedIdentity(t *testing.T) {
	const allocationID = "22222222-2222-4222-8222-222222222222"
	tests := map[string]struct {
		gameServerStatus     int
		gameServerUID        string
		labelAllocation      string
		annotationAllocation string
		podStatus            int
		podOwnerUID          string
	}{
		"gameserver missing":          {gameServerStatus: http.StatusNotFound},
		"same name new uid":           {gameServerStatus: http.StatusOK, gameServerUID: "gs-new", labelAllocation: allocationID, podStatus: http.StatusOK, podOwnerUID: "gs-new"},
		"allocation label drift":      {gameServerStatus: http.StatusOK, gameServerUID: "gs-old", labelAllocation: "33333333-3333-4333-8333-333333333333", podStatus: http.StatusOK, podOwnerUID: "gs-old"},
		"allocation annotation drift": {gameServerStatus: http.StatusOK, gameServerUID: "gs-old", labelAllocation: allocationID, annotationAllocation: "33333333-3333-4333-8333-333333333333", podStatus: http.StatusOK, podOwnerUID: "gs-old"},
		"owned pod missing":           {gameServerStatus: http.StatusOK, gameServerUID: "gs-old", labelAllocation: allocationID, podStatus: http.StatusNotFound},
		"pod owner uid drift":         {gameServerStatus: http.StatusOK, gameServerUID: "gs-old", labelAllocation: allocationID, podStatus: http.StatusOK, podOwnerUID: "gs-new"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("preflight issued side effect method=%s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				if strings.Contains(r.URL.Path, "/pods/") {
					status := tc.podStatus
					if status == 0 {
						status = http.StatusOK
					}
					if status != http.StatusOK {
						w.WriteHeader(status)
						return
					}
					writeOwnedPod(w, "battle-legacy-2", "pod-current", tc.podOwnerUID)
					return
				}
				status := tc.gameServerStatus
				if status == 0 {
					status = http.StatusOK
				}
				if status != http.StatusOK {
					w.WriteHeader(status)
					return
				}
				annotationAllocation := tc.annotationAllocation
				if annotationAllocation == "" {
					annotationAllocation = allocationID
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
					"name": "battle-legacy-2", "uid": tc.gameServerUID, "resourceVersion": "101",
					"labels":      map[string]string{"pandora.dev/allocation-id": tc.labelAllocation},
					"annotations": map[string]string{"pandora.dev/allocation-id": annotationAllocation},
				}})
			}))
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			if podUID, err := a.ResolveExpectedPodUID(context.Background(), &AuthoritativeGameServerAllocation{
				PodName: "battle-legacy-2", InstanceUID: "gs-old", AllocationID: allocationID,
			}); err == nil || podUID != "" {
				t.Fatalf("unsafe legacy identity accepted: pod_uid=%q err=%v", podUID, err)
			}
		})
	}
}

// TestProbeExpectedInstanceGone:warming 冷加载宽限的判死探测。只有「物理消失双确认」
// 或「Agones 依据 SDK health ping 判 Unhealthy」返回 true;存活/读失败一律不判死。
func TestProbeExpectedInstanceGone(t *testing.T) {
	const podName = "battle-probe-1"
	tests := map[string]struct {
		gsStatus  int    // GameServer GET http 状态(0=200)
		gsUID     string // 200 时返回的 uid
		gsState   string // 200 时返回的 status.state
		podStatus int    // Pod GET http 状态(0=200)
		podUID    string // 200 时返回的 pod uid
		argPodUID string // 探测入参 podUID
		wantGone  bool
		wantErr   bool
	}{
		"alive scheduled":              {gsUID: "gs-1", gsState: "Scheduled", argPodUID: "pod-1", wantGone: false},
		"alive allocated cold loading": {gsUID: "gs-1", gsState: "Allocated", argPodUID: "pod-1", wantGone: false},
		"unhealthy verdict":            {gsUID: "gs-1", gsState: "Unhealthy", argPodUID: "pod-1", wantGone: true},
		"gs gone pod gone":             {gsStatus: http.StatusNotFound, podStatus: http.StatusNotFound, argPodUID: "pod-1", wantGone: true},
		"gs gone pod uid replaced":     {gsStatus: http.StatusNotFound, podUID: "pod-new", argPodUID: "pod-1", wantGone: true},
		"gs gone pod still present":    {gsStatus: http.StatusNotFound, podUID: "pod-1", argPodUID: "pod-1", wantGone: false},
		"gs uid replaced pod gone":     {gsUID: "gs-new", gsState: "Ready", podStatus: http.StatusNotFound, argPodUID: "pod-1", wantGone: true},
		"gs gone no durable pod uid":   {gsStatus: http.StatusNotFound, argPodUID: "", wantGone: true},
		"gs read failure":              {gsStatus: http.StatusInternalServerError, argPodUID: "pod-1", wantErr: true},
		"pod read failure":             {gsStatus: http.StatusNotFound, podStatus: http.StatusInternalServerError, argPodUID: "pod-1", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var writes atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				if strings.Contains(r.URL.Path, "/pods/") {
					status := tc.podStatus
					if status == 0 {
						status = http.StatusOK
					}
					if status != http.StatusOK {
						w.WriteHeader(status)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
						"name": podName, "uid": tc.podUID,
					}})
					return
				}
				status := tc.gsStatus
				if status == 0 {
					status = http.StatusOK
				}
				if status != http.StatusOK {
					w.WriteHeader(status)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metadata": map[string]any{"name": podName, "uid": tc.gsUID, "resourceVersion": "1"},
					"status":   map[string]any{"state": tc.gsState},
				})
			}))
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			gone, err := a.ProbeExpectedInstanceGone(context.Background(), podName, "gs-1", tc.argPodUID)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got gone=%v", gone)
				}
			} else if err != nil || gone != tc.wantGone {
				t.Fatalf("gone=%v err=%v want gone=%v", gone, err, tc.wantGone)
			}
			if writes.Load() != 0 {
				t.Fatalf("probe issued %d non-GET side effects", writes.Load())
			}
		})
	}
}

// TestReleaseExpectedDeletionGraceContract:exact 回收对 deletionTimestamp 的三态契约
//(INC-20260727-001 复审 P1-2)。已受理删除不重复 DELETE、宽限内快速返回 pending 哨兵、
// 双对象物理消失才 nil(teardown proof 门),同名新 UID 零删除。
func TestReleaseExpectedDeletionGraceContract(t *testing.T) {
	const podName = "battle-grace-1"
	alloc := func() *AuthoritativeGameServerAllocation {
		return &AuthoritativeGameServerAllocation{
			PodName: podName, InstanceUID: "gs-1", PodUID: "pod-1",
			AllocationID: "44444444-4444-4444-8444-444444444444",
		}
	}
	type objectState struct {
		status   int    // 0=200
		uid      string
		deleting bool
	}
	writeObject := func(w http.ResponseWriter, st objectState) {
		if st.status != 0 && st.status != http.StatusOK {
			w.WriteHeader(st.status)
			return
		}
		meta := map[string]any{"name": podName, "uid": st.uid}
		if st.deleting {
			meta["deletionTimestamp"] = "2026-07-27T14:00:00Z"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": meta})
	}
	tests := map[string]struct {
		gs          objectState
		pod         objectState
		gsAfterDel  *objectState // DELETE 受理后的 gs 状态(nil=不变)
		wantPending bool
		wantNil     bool
		wantDeletes int32
	}{
		"live then accepted grace": {
			gs: objectState{uid: "gs-1"}, pod: objectState{uid: "pod-1"},
			gsAfterDel:  &objectState{uid: "gs-1", deleting: true},
			wantPending: true, wantDeletes: 1,
		},
		"already deleting no repeat delete": {
			gs: objectState{uid: "gs-1", deleting: true}, pod: objectState{uid: "pod-1", deleting: true},
			wantPending: true, wantDeletes: 0,
		},
		"gs gone pod in grace": {
			gs: objectState{status: http.StatusNotFound}, pod: objectState{uid: "pod-1", deleting: true},
			wantPending: true, wantDeletes: 0,
		},
		"both physically gone": {
			gs: objectState{status: http.StatusNotFound}, pod: objectState{status: http.StatusNotFound},
			wantNil: true, wantDeletes: 0,
		},
		"same name new uid zero delete": {
			gs: objectState{uid: "gs-new"}, pod: objectState{uid: "pod-new"},
			wantNil: true, wantDeletes: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var deletes atomic.Int32
			var deleted atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deletes.Add(1)
					deleted.Store(true)
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method != http.MethodGet {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				if strings.Contains(r.URL.Path, "/pods/") {
					writeObject(w, tc.pod)
					return
				}
				gs := tc.gs
				if deleted.Load() && tc.gsAfterDel != nil {
					gs = *tc.gsAfterDel
				}
				writeObject(w, gs)
			}))
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			err := a.ReleaseExpected(context.Background(), alloc())
			switch {
			case tc.wantNil:
				if err != nil {
					t.Fatalf("want physical-gone success, err=%v", err)
				}
			case tc.wantPending:
				if !errors.Is(err, ErrReleaseDeletionPending) {
					t.Fatalf("want deletion-pending sentinel, err=%v", err)
				}
			default:
				if err == nil {
					t.Fatal("want error")
				}
			}
			if got := deletes.Load(); got != tc.wantDeletes {
				t.Fatalf("DELETE calls=%d want %d", got, tc.wantDeletes)
			}
		})
	}
}

// allocation-id 回退路径的删除宽限契约(复审必修):LIST 先行,对象全部处于删除宽限时
// 直接 pending 且**不重复 DeleteCollection**;空集合保留 DeleteCollection+后置 LIST 的
// timeout-late-apply 防线。
func TestReleaseExpectedAllocationIDCollectionGrace(t *testing.T) {
	const allocationID = "55555555-5555-4555-8555-555555555555"
	writeList := func(w http.ResponseWriter, deleting bool, count int) {
		items := make([]map[string]any, 0, count)
		for i := 0; i < count; i++ {
			meta := map[string]any{"name": "gs-col", "uid": "u1"}
			if deleting {
				meta["deletionTimestamp"] = "2026-07-27T14:00:00Z"
			}
			items = append(items, map[string]any{"metadata": meta})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	}
	t.Run("consecutive calls single delete", func(t *testing.T) {
		var deletes atomic.Int32
		var deleted atomic.Bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				writeList(w, deleted.Load(), 1)
			case http.MethodDelete:
				deletes.Add(1)
				deleted.Store(true)
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}))
		defer srv.Close()
		a := newTestAllocator(t, srv.URL)
		alloc := &AuthoritativeGameServerAllocation{AllocationID: allocationID} // InstanceUID 空 → collection 路径
		if err := a.ReleaseExpected(context.Background(), alloc); !errors.Is(err, ErrReleaseDeletionPending) {
			t.Fatalf("call 1 want pending, err=%v", err)
		}
		if err := a.ReleaseExpected(context.Background(), alloc); !errors.Is(err, ErrReleaseDeletionPending) {
			t.Fatalf("call 2 want pending, err=%v", err)
		}
		if got := deletes.Load(); got != 1 {
			t.Fatalf("DeleteCollection calls=%d want 1(grace 内不得重复)", got)
		}
	})
	t.Run("empty collection keeps late-apply defense", func(t *testing.T) {
		var deletes atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				writeList(w, false, 0)
			case http.MethodDelete:
				deletes.Add(1)
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}))
		defer srv.Close()
		a := newTestAllocator(t, srv.URL)
		if err := a.ReleaseExpected(context.Background(),
			&AuthoritativeGameServerAllocation{AllocationID: allocationID}); err != nil {
			t.Fatalf("empty collection release: %v", err)
		}
		if got := deletes.Load(); got != 1 {
			t.Fatalf("empty collection must keep idempotent DeleteCollection defense, calls=%d", got)
		}
	})
}

func TestAllocateAuthoritative_StrictGETRejectsMissingIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
				"state": "Allocated", "gameServerName": "battle-fleet-bad",
				"address": "10.0.0.8", "ports": []map[string]any{{"port": 7777}},
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
			"name": "battle-fleet-bad", "uid": "", "resourceVersion": "",
		}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if _, err := a.AllocateAuthoritative(context.Background(), 42, "11111111-1111-4111-8111-111111111111", []uint64{1}, nil, 1, "ranked", "stable"); err == nil {
		t.Fatal("missing UID/RV must fail closed")
	}
}

func TestAllocateAuthoritative_POSTUnknownReturnsAllocationFence(t *testing.T) {
	a := newTestAllocator(t, "http://127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const allocationID = "22222222-2222-4222-8222-222222222222"
	got, err := a.AllocateAuthoritative(ctx, 42, allocationID, []uint64{1}, nil, 1, "ranked", "stable")
	if err == nil || got == nil || got.AllocationID != allocationID || got.InstanceUID != "" {
		t.Fatalf("partial=%+v err=%v", got, err)
	}
}

func TestResolveAllocationByID_UniqueExactGameServerAndPod(t *testing.T) {
	const allocationID = "44444444-4444-4444-8444-444444444444"
	var sawSelector atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pods/") {
			writeOwnedPod(w, "battle-reconcile-1", "pod-uid-reconcile-1", "gs-uid-reconcile-1")
			return
		}
		if r.Method != http.MethodGet ||
			r.URL.Path != "/apis/agones.dev/v1/namespaces/pandora/gameservers" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("labelSelector") == "pandora.dev/allocation-id="+allocationID {
			sawSelector.Store(true)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
			"metadata": map[string]any{
				"name": "battle-reconcile-1", "uid": "gs-uid-reconcile-1", "resourceVersion": "301",
				"labels": map[string]string{
					"pandora.dev/match-id": "42", "pandora.dev/map-id": "1",
					"pandora.dev/game-mode": "ranked", "pandora.dev/allocation-id": allocationID,
					"pandora.dev/release-track": "stable",
				},
				"annotations": map[string]string{
					"pandora.dev/allocation-id": allocationID, "pandora.dev/roster": "1,2",
					"pandora.dev/combat-factions": "1=2,2=7",
					"pandora.dev/release-track":   "stable",
				},
			},
		}}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	got, found, err := a.ResolveAllocationByID(context.Background(), 42, allocationID,
		[]uint64{2, 1, 2}, map[uint64]uint32{1: 2, 2: 7}, 1, "ranked")
	if err != nil || !found || got == nil {
		t.Fatalf("ResolveAllocationByID found=%t allocation=%+v err=%v", found, got, err)
	}
	if !sawSelector.Load() || got.PodName != "battle-reconcile-1" ||
		got.InstanceUID != "gs-uid-reconcile-1" || got.PodUID != "pod-uid-reconcile-1" ||
		got.ResourceVersion != "301" || got.AllocationID != allocationID || got.ReleaseTrack != "stable" {
		t.Fatalf("resolved exact allocation=%+v selector=%t", got, sawSelector.Load())
	}
}

func TestResolveAllocationByIDRejectsCombatFactionDrift(t *testing.T) {
	const allocationID = "45454545-4545-4545-8545-454545454545"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{
			"metadata": map[string]any{
				"name": "battle-faction-drift", "uid": "uid-faction-drift", "resourceVersion": "301",
				"labels": map[string]string{
					"pandora.dev/match-id": "42", "pandora.dev/map-id": "1",
					"pandora.dev/game-mode": "ranked", battleAllocationMetadataKey: allocationID,
					releaseTrackMetadataKey: "stable",
				},
				"annotations": map[string]string{
					battleAllocationMetadataKey: allocationID, battleRosterAnnotationKey: "1",
					battleCombatFactionsAnnotationKey: "1=9", releaseTrackMetadataKey: "stable",
				},
			},
		}}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	got, found, err := a.ResolveAllocationByID(context.Background(), 42, allocationID,
		[]uint64{1}, map[uint64]uint32{1: 2}, 1, "ranked")
	if err == nil || found || got != nil {
		t.Fatalf("combat faction drift accepted: found=%t allocation=%+v err=%v", found, got, err)
	}
}

func TestResolveAllocationByID_EmptyIsAuthoritativeAbsence(t *testing.T) {
	const allocationID = "55555555-5555-4555-8555-555555555555"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	got, found, err := a.ResolveAllocationByID(context.Background(), 43, allocationID,
		[]uint64{1}, nil, 1, "ranked")
	if err != nil || found || got != nil {
		t.Fatalf("empty allocation resolution found=%t allocation=%+v err=%v", found, got, err)
	}
}

func TestResolveAllocationByID_MultipleOrAPIUnknownFailClosed(t *testing.T) {
	const allocationID = "66666666-6666-4666-8666-666666666666"
	for _, tc := range []struct {
		name   string
		status int
		body   any
	}{
		{name: "multiple", status: http.StatusOK, body: map[string]any{"items": []any{
			map[string]any{"metadata": map[string]any{"name": "a"}},
			map[string]any{"metadata": map[string]any{"name": "b"}},
		}}},
		{name: "api_unknown", status: http.StatusServiceUnavailable, body: map[string]any{"error": "retry"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(tc.body)
			}))
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			if got, found, err := a.ResolveAllocationByID(context.Background(), 44, allocationID,
				[]uint64{1}, nil, 1, "ranked"); err == nil || found || got != nil {
				t.Fatalf("ambiguous/unknown result found=%t allocation=%+v err=%v", found, got, err)
			}
		})
	}
}

func TestAllocateAuthoritative_CanaryNoCapacityFallsBackAndPersistsGETTrack(t *testing.T) {
	const allocationID = "33333333-3333-4333-8333-333333333333"
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			call := posts.Add(1)
			body, _ := io.ReadAll(r.Body)
			if call == 1 {
				if !strings.Contains(string(body), `"agones.dev/fleet":"battle-canary"`) ||
					!strings.Contains(string(body), `"pandora.dev/release-track":"canary"`) {
					t.Errorf("first request is not exact canary: %s", body)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"state": "UnAllocated"}})
				return
			}
			if !strings.Contains(string(body), `"agones.dev/fleet":"battle-stable"`) ||
				!strings.Contains(string(body), `"pandora.dev/release-track":"stable"`) {
				t.Errorf("fallback request is not exact stable: %s", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
				"state": "Allocated", "gameServerName": "battle-stable-1", "address": "10.0.0.9",
				"ports": []map[string]any{{"port": 7777}},
			}})
		case http.MethodGet:
			if strings.Contains(r.URL.Path, "/pods/") {
				writeOwnedPod(w, "battle-stable-1", "pod-uid-stable-1", "uid-stable-1")
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
				"name": "battle-stable-1", "uid": "uid-stable-1", "resourceVersion": "7",
				"labels": map[string]string{
					"pandora.dev/match-id": "77", battleAllocationMetadataKey: allocationID,
					releaseTrackMetadataKey: "stable",
				},
				"annotations": map[string]string{
					battleAllocationMetadataKey: allocationID, battleRosterAnnotationKey: "10,20",
					releaseTrackMetadataKey: "stable",
				},
			}})
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()
	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		APIServer: srv.URL, Namespace: "pandora", FleetName: "battle-stable",
		CanaryFleetName: "battle-canary", CanaryPercent: 10, CanarySeed: "release-seed", TokenPath: "-",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.AllocateAuthoritative(context.Background(), 77, allocationID,
		[]uint64{20, 10}, nil, 1, "ranked", "canary")
	if err != nil || got == nil || got.ReleaseTrack != "stable" || got.PodName != "battle-stable-1" {
		t.Fatalf("fallback allocation=%+v err=%v", got, err)
	}
	if posts.Load() != 2 {
		t.Fatalf("POST calls=%d want 2", posts.Load())
	}
}

func TestDeliverCredential_StrictGETConfirmationMatrix(t *testing.T) {
	tests := []struct {
		name       string
		patchCode  int
		patchDelay time.Duration
		applied    bool
		wantOK     bool
	}{
		{name: "2xx_bad_body_but_applied", patchCode: http.StatusOK, applied: true, wantOK: true},
		{name: "409_already_applied", patchCode: http.StatusConflict, applied: true, wantOK: true},
		{name: "409_wrong_object", patchCode: http.StatusConflict, applied: false, wantOK: false},
		{name: "2xx_without_expected_annotations", patchCode: http.StatusOK, applied: false, wantOK: false},
		{name: "transport_timeout_but_applied", patchCode: http.StatusOK, patchDelay: 80 * time.Millisecond, applied: true, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var patchSeen atomic.Bool
			annotations := map[string]string{
				"pandora.dev/ds-token": "jwt-value", "pandora.dev/ds-token-jti": "jti-1",
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPatch:
					patchSeen.Store(true)
					if got := r.Header.Get("Content-Type"); got != "application/json-patch+json" {
						t.Errorf("patch content-type=%q", got)
					}
					body, _ := io.ReadAll(r.Body)
					if !strings.Contains(string(body), `"path":"/metadata/uid"`) ||
						!strings.Contains(string(body), `"path":"/metadata/resourceVersion"`) {
						t.Errorf("patch missing UID/RV tests: %s", body)
					}
					if tc.patchDelay > 0 {
						time.Sleep(tc.patchDelay)
					}
					w.WriteHeader(tc.patchCode)
					_, _ = w.Write([]byte("not-a-k8s-object"))
				case http.MethodGet:
					gotAnnotations := map[string]string{"pandora.dev/ds-token": "wrong"}
					if tc.applied {
						gotAnnotations = annotations
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
						"name": "battle-fleet-auth1", "uid": "uid-auth1",
						"resourceVersion": "102", "annotations": gotAnnotations,
					}})
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
			}))
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			if tc.patchDelay > 0 {
				a.allocateTimeout = 20 * time.Millisecond
			}
			allocation := &AuthoritativeGameServerAllocation{
				PodName: "battle-fleet-auth1", InstanceUID: "uid-auth1",
				ResourceVersion: "101", AnnotationsPresent: true,
			}
			rv, err := a.DeliverCredential(context.Background(), allocation, annotations)
			if tc.wantOK && (err != nil || rv != "102") {
				t.Fatalf("DeliverCredential: rv=%q err=%v", rv, err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("DeliverCredential unexpectedly succeeded rv=%q", rv)
			}
			if !patchSeen.Load() {
				t.Fatal("PATCH not sent")
			}
		})
	}
}

func TestDeliverCredential_ConfirmationSurvivesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	annotations := map[string]string{
		"pandora.dev/ds-token":     "jwt-value",
		"pandora.dev/ds-token-jti": "jti-1",
	}
	var patchSeen atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPatch:
			patchSeen.Store(true)
			// 模拟入站 RPC 在 PATCH 已被 apiserver 应用后恰好取消。确认 GET 必须使用
			// 独立有界上下文，否则会把真实已应用误报为未知并留下永久 pending。
			cancel()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
				"name": "battle-fleet-auth1", "uid": "uid-auth1",
				"resourceVersion": "102", "annotations": annotations,
			}})
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	rv, err := a.DeliverCredential(ctx, &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-auth1", InstanceUID: "uid-auth1",
		ResourceVersion: "101", AnnotationsPresent: true,
	}, annotations)
	if err != nil || rv != "102" {
		t.Fatalf("DeliverCredential: rv=%q err=%v", rv, err)
	}
	if !patchSeen.Load() {
		t.Fatal("PATCH not sent")
	}
}

func TestAgonesAllocate_NoAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{"state": "UnAllocated"},
		})
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	_, _, _, err := a.Allocate(context.Background(), 1, 1, "moba", "stable")
	if err == nil {
		t.Fatal("expected ErrDSNoAvailable, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrDSNoAvailable {
		t.Errorf("code: got %d want ErrDSNoAvailable(5001)", got)
	}
}

// TestAgonesAllocate_MapFleetSelectorOrder 校验混合形态路由:
//   - map_id 命中 map_fleets → selectors 有序 [专属预热 Fleet, 通用 Fleet](Agones 按序尝试);
//   - 未命中 → 仅通用 Fleet 一个 selector(行为与未配置 map_fleets 完全一致)。
func TestAgonesAllocate_MapFleetSelectorOrder(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"state":          "Allocated",
				"gameServerName": "songlin-fleet-x1",
				"address":        "10.0.0.9",
				"ports":          []map[string]any{{"name": "default", "port": 7788}},
			},
		})
	}))
	defer srv.Close()

	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		Enabled:   true,
		APIServer: srv.URL,
		Namespace: "pandora",
		FleetName: "battle-fleet",
		TokenPath: "-",
		MapFleets: []conf.AgonesMapFleet{{MapID: 7, FleetName: "songlin-fleet"}},
	})
	if err != nil {
		t.Fatalf("NewAgonesGameServerAllocator: %v", err)
	}

	// 命中 map_id=7:两个 selector,专属在前、通用兜底在后。
	if _, _, _, err := a.Allocate(context.Background(), 1, 7, "pve_coop", "stable"); err != nil {
		t.Fatalf("Allocate(map 7): %v", err)
	}
	var req struct {
		Spec struct {
			Selectors []struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selectors"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if len(req.Spec.Selectors) != 2 {
		t.Fatalf("selectors: got %d want 2 (dedicated first, generic fallback)", len(req.Spec.Selectors))
	}
	if got := req.Spec.Selectors[0].MatchLabels["agones.dev/fleet"]; got != "songlin-fleet" {
		t.Errorf("selector[0]: got %q want songlin-fleet", got)
	}
	if got := req.Spec.Selectors[1].MatchLabels["agones.dev/fleet"]; got != "battle-fleet" {
		t.Errorf("selector[1]: got %q want battle-fleet", got)
	}

	// 未命中 map_id=6:只有通用 selector。
	if _, _, _, err := a.Allocate(context.Background(), 2, 6, "pvp_5v5", "stable"); err != nil {
		t.Fatalf("Allocate(map 6): %v", err)
	}
	if err := json.Unmarshal(gotBody, &req); err != nil {
		t.Fatalf("unmarshal request body: %v", err)
	}
	if len(req.Spec.Selectors) != 1 {
		t.Fatalf("selectors: got %d want 1 (generic only)", len(req.Spec.Selectors))
	}
	if got := req.Spec.Selectors[0].MatchLabels["agones.dev/fleet"]; got != "battle-fleet" {
		t.Errorf("selector[0]: got %q want battle-fleet", got)
	}
}

func TestAgonesAllocate_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	_, _, _, err := a.Allocate(context.Background(), 1, 1, "moba", "stable")
	if err == nil {
		t.Fatal("expected ErrDSAllocationFailed, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrDSAllocationFailed {
		t.Errorf("code: got %d want ErrDSAllocationFailed(5002)", got)
	}
}

func TestAgonesRelease_OK(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	if err := a.Release(context.Background(), "battle-fleet-abc12"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method: got %s want DELETE", gotMethod)
	}
	wantPath := "/apis/agones.dev/v1/namespaces/pandora/gameservers/battle-fleet-abc12"
	if gotPath != wantPath {
		t.Errorf("path: got %s want %s", gotPath, wantPath)
	}
}

func TestAgonesReleaseExpected_UsesUIDPrecondition(t *testing.T) {
	// 有状态假 apiserver:删除受理前 exact GameServer 存活(触发真 DELETE 并校验
	// precondition),受理后双对象 404(物理消失确认)。原静态 404 版本在删除前预检
	//(deletionTimestamp 三态)引入后不再触发 DELETE——对象已消失本就无需删。
	var gotUID string
	var deleted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if deleted.Load() {
				http.Error(w, "gone", http.StatusNotFound)
				return
			}
			if strings.Contains(r.URL.Path, "/pods/") {
				_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
					"name": "battle-fleet-abc12", "uid": "pod-uid-old",
				}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
				"name": "battle-fleet-abc12", "uid": "uid-old",
			}})
			return
		}
		var body struct {
			Preconditions struct {
				UID string `json:"uid"`
			} `json:"preconditions"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotUID = body.Preconditions.UID
		deleted.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-abc12", InstanceUID: "uid-old", PodUID: "pod-uid-old",
	}); err != nil {
		t.Fatalf("ReleaseExpected: %v", err)
	}
	if gotUID != "uid-old" {
		t.Fatalf("delete uid precondition=%q, want uid-old", gotUID)
	}
}

func TestAgonesReleaseExpected_MissingDurablePodUIDHasZeroDeleteSideEffects(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-legacy", InstanceUID: "uid-old",
	})
	if err == nil || errcode.As(err) != errcode.ErrDSAllocationFailed {
		t.Fatalf("missing durable Pod UID err=%v code=%v", err, errcode.As(err))
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("missing durable Pod UID made %d Kubernetes requests; want zero", got)
	}
}

func TestAgonesReleaseExpected_UIDConflictDoesNotSucceed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "UID precondition failed", http.StatusConflict)
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	a.allocateTimeout = 20 * time.Millisecond
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-rebuilt", InstanceUID: "uid-old", PodUID: "pod-uid-old",
	}); err == nil {
		t.Fatal("same-name rebuilt GameServer UID conflict must not be treated as released")
	}
}

func TestAgonesReleaseExpected_2xxWaitsForGameServerAndPodUIDGone(t *testing.T) {
	var deleted atomic.Bool
	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted.Store(true)
			w.WriteHeader(http.StatusOK)
			return
		}
		call := gets.Add(1)
		// 删除请求后的第一轮 GET 仍看到 exact old UIDs；这不能
		// 落 teardown proof。后续两个对象都 404 才返回成功。
		if call <= 2 {
			uid := "uid-old"
			if strings.Contains(r.URL.Path, "/pods/") {
				uid = "pod-uid-old"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid}})
			return
		}
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	a.allocateTimeout = 500 * time.Millisecond
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-abc12", InstanceUID: "uid-old", PodUID: "pod-uid-old",
	}); err != nil {
		t.Fatalf("ReleaseExpected: %v", err)
	}
	if !deleted.Load() || gets.Load() < 4 {
		t.Fatalf("delete=%t get_calls=%d want post-delete polling", deleted.Load(), gets.Load())
	}
}

func TestAgonesReleaseExpected_2xxButOldPodStillExistsFailsClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK)
			return
		}
		uid := "uid-old"
		if strings.Contains(r.URL.Path, "/pods/") {
			uid = "pod-uid-old"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	a.allocateTimeout = 20 * time.Millisecond
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "battle-fleet-abc12", InstanceUID: "uid-old", PodUID: "pod-uid-old",
	}); err == nil {
		t.Fatal("DELETE 2xx while exact old Pod UID remains must fail closed")
	}
}

func TestAgonesReleaseExpected_UnknownUIDUsesAllocationLabel(t *testing.T) {
	var gotPath, gotSelector string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSelector = r.URL.Query().Get("labelSelector")
		if r.Method == http.MethodDelete {
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), "preconditions") {
				t.Errorf("unknown UID collection delete must not invent UID precondition: %s", body)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		PodName: "do-not-delete-by-name", AllocationID: "28888c9f-0289-47dd-855f-55ff163e70a0",
	}); err != nil {
		t.Fatalf("ReleaseExpected by allocation label: %v", err)
	}
	if gotPath != "/apis/agones.dev/v1/namespaces/pandora/gameservers" ||
		gotSelector != "pandora.dev/allocation-id=28888c9f-0289-47dd-855f-55ff163e70a0" {
		t.Fatalf("collection delete path=%q selector=%q", gotPath, gotSelector)
	}
}

func TestAgonesReleaseExpected_UnknownUIDRequiresEmptyListConfirmation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK) // 2xx 不能冒充已删除
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{
			map[string]any{"metadata": map[string]any{"name": "still-there"}},
		}})
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if err := a.ReleaseExpected(context.Background(), &AuthoritativeGameServerAllocation{
		AllocationID: "5c51f910-87b8-4bb3-8629-06d01a09ab09",
	}); err == nil {
		t.Fatal("2xx DeleteCollection with remaining object must fail closed")
	}
}

func TestAgonesRelease_NotFoundIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	if err := a.Release(context.Background(), "ghost-gs"); err != nil {
		t.Fatalf("Release on 404 should be nil(idempotent), got %v", err)
	}
}

func TestAgonesRelease_EmptyPodNoop(t *testing.T) {
	a := newTestAllocator(t, "http://127.0.0.1:1") // 不会被调用
	if err := a.Release(context.Background(), ""); err != nil {
		t.Fatalf("Release(\"\") should be noop nil, got %v", err)
	}
}

func TestSanitizeLabelValue(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"moba_5v5", "moba_5v5"},
		{" mode/5v5 ", "mode-5v5"},
		{"---", "unknown"},
		{strings.Repeat("a", 70), strings.Repeat("a", 63)},
	}
	for _, c := range cases {
		if got := sanitizeLabelValue(c.in); got != c.want {
			t.Errorf("sanitizeLabelValue(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

// TestWatchedFleets 校验容量巡检盯的 Fleet 集合:通用池在前 + 专属池去重按字典序。
func TestWatchedFleets(t *testing.T) {
	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		Enabled:   true,
		APIServer: "http://127.0.0.1:1",
		Namespace: "pandora",
		FleetName: "battle-fleet",
		TokenPath: "-",
		MapFleets: []conf.AgonesMapFleet{
			{MapID: 7, FleetName: "songlin-fleet"},
			{MapID: 8, FleetName: "arena-fleet"},
			{MapID: 9, FleetName: "battle-fleet"}, // 与通用池重名 → 去重
		},
	})
	if err != nil {
		t.Fatalf("NewAgonesGameServerAllocator: %v", err)
	}
	got := a.WatchedFleets()
	want := []string{"battle-fleet", "arena-fleet", "songlin-fleet"}
	if len(got) != len(want) {
		t.Fatalf("WatchedFleets len: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("WatchedFleets[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

// TestListFleetCapacities_OK 解析 Fleet status 容量快照,并对每个受管 Fleet 各发一次 GET。
func TestListFleetCapacities_OK(t *testing.T) {
	byFleet := map[string]map[string]any{
		"battle-fleet":  {"replicas": 10, "readyReplicas": 3, "allocatedReplicas": 7},
		"songlin-fleet": {"replicas": 4, "readyReplicas": 0, "allocatedReplicas": 4},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s want GET", r.Method)
		}
		// 路径尾段是 fleet 名
		parts := strings.Split(r.URL.Path, "/")
		fleet := parts[len(parts)-1]
		st, ok := byFleet[fleet]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": st})
	}))
	defer srv.Close()

	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		Enabled:   true,
		APIServer: srv.URL,
		Namespace: "pandora",
		FleetName: "battle-fleet",
		TokenPath: "-",
		MapFleets: []conf.AgonesMapFleet{{MapID: 7, FleetName: "songlin-fleet"}},
	})
	if err != nil {
		t.Fatalf("NewAgonesGameServerAllocator: %v", err)
	}

	caps, err := a.ListFleetCapacities(context.Background())
	if err != nil {
		t.Fatalf("ListFleetCapacities: %v", err)
	}
	if len(caps) != 2 {
		t.Fatalf("caps len: got %d want 2", len(caps))
	}
	got := map[string]FleetCapacity{}
	for _, c := range caps {
		got[c.Fleet] = c
	}
	if c := got["battle-fleet"]; c.Replicas != 10 || c.Ready != 3 || c.Allocated != 7 {
		t.Errorf("battle-fleet: got %+v want replicas=10 ready=3 allocated=7", c)
	}
	if c := got["songlin-fleet"]; c.Replicas != 4 || c.Ready != 0 || c.Allocated != 4 {
		t.Errorf("songlin-fleet: got %+v want replicas=4 ready=0 allocated=4", c)
	}
}

// TestListFleetCapacities_PartialFailure 单 Fleet 5xx 不影响其余,错误经 error 汇总返回。
func TestListFleetCapacities_PartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "songlin-fleet") {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{"replicas": 5, "readyReplicas": 5, "allocatedReplicas": 0},
		})
	}))
	defer srv.Close()

	a, err := NewAgonesGameServerAllocator(conf.AgonesConf{
		Enabled:   true,
		APIServer: srv.URL,
		Namespace: "pandora",
		FleetName: "battle-fleet",
		TokenPath: "-",
		MapFleets: []conf.AgonesMapFleet{{MapID: 7, FleetName: "songlin-fleet"}},
	})
	if err != nil {
		t.Fatalf("NewAgonesGameServerAllocator: %v", err)
	}

	caps, err := a.ListFleetCapacities(context.Background())
	if err == nil {
		t.Fatal("expected aggregated error for failing fleet, got nil")
	}
	if len(caps) != 1 || caps[0].Fleet != "battle-fleet" {
		t.Fatalf("expected 1 successful cap(battle-fleet), got %+v", caps)
	}
}
