// agones_orphan_test.go — 孤儿 Allocated GameServer 列举与 exact 复核删除(2026-08-03)。
package data

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func gsListItem(name, uid, fleet, allocID, state, deletionTS string) map[string]any {
	labels := map[string]any{fleetLabelKey: fleet}
	if allocID != "" {
		labels[battleAllocationMetadataKey] = allocID
	}
	meta := map[string]any{"name": name, "uid": uid, "resourceVersion": "100", "labels": labels}
	if deletionTS != "" {
		meta["deletionTimestamp"] = deletionTS
	}
	return map[string]any{"metadata": meta, "status": map[string]any{"state": state}}
}

func TestListAllocatedGameServersFiltersStateAndMapsFields(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet ||
			r.URL.Path != "/apis/agones.dev/v1/namespaces/pandora/gameservers" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		gotQuery = r.URL.Query().Get("labelSelector")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{
			gsListItem("gs-a", "uid-a", "battle-fleet", "alloc-a", agonesStateAllocated, ""),
			gsListItem("gs-ready", "uid-r", "battle-fleet", "", "Ready", ""),
			gsListItem("gs-grace", "uid-g", "battle-fleet", "alloc-g", agonesStateAllocated,
				"2026-08-03T00:00:00Z"),
		}})
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	out, err := a.ListAllocatedGameServers(t.Context())
	if err != nil {
		t.Fatalf("ListAllocatedGameServers: %v", err)
	}
	if !strings.Contains(gotQuery, fleetLabelKey+" in (") ||
		!strings.Contains(gotQuery, "battle-fleet") {
		t.Fatalf("labelSelector must scope to watched fleets, got %q", gotQuery)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 allocated gameservers (Ready filtered), got %+v", out)
	}
	if out[0].Name != "gs-a" || out[0].UID != "uid-a" ||
		out[0].Fleet != "battle-fleet" || out[0].AllocationID != "alloc-a" || out[0].Deleting {
		t.Fatalf("field mapping wrong: %+v", out[0])
	}
	if out[1].Name != "gs-grace" || !out[1].Deleting {
		t.Fatalf("deletionTimestamp must map to Deleting: %+v", out[1])
	}
}

// 分页回归(2026-08-03 补审 P2):单次响应撑不下全部 GameServer 时必须逐页取全,
// 而不是只拿第一页(半份清单会把仍被引用的 GS 误判为孤儿)。
func TestListAllocatedGameServersFollowsContinuePages(t *testing.T) {
	var gotLimits, gotContinues []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimits = append(gotLimits, r.URL.Query().Get("limit"))
		cont := r.URL.Query().Get("continue")
		gotContinues = append(gotContinues, cont)
		switch cont {
		case "":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"continue": "token-page-2"},
				"items": []any{
					gsListItem("gs-p1", "uid-p1", "battle-fleet", "alloc-p1", agonesStateAllocated, ""),
				}})
		case "token-page-2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"continue": ""},
				"items": []any{
					gsListItem("gs-p2", "uid-p2", "battle-fleet", "alloc-p2", agonesStateAllocated, ""),
					gsListItem("gs-p2-ready", "uid-p2r", "battle-fleet", "", "Ready", ""),
				}})
		default:
			t.Errorf("unexpected continue token %q", cont)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	out, err := a.ListAllocatedGameServers(t.Context())
	if err != nil {
		t.Fatalf("ListAllocatedGameServers: %v", err)
	}
	if len(out) != 2 || out[0].Name != "gs-p1" || out[1].Name != "gs-p2" {
		t.Fatalf("must collect Allocated across all pages, got %+v", out)
	}
	if len(gotContinues) != 2 || gotContinues[0] != "" || gotContinues[1] != "token-page-2" {
		t.Fatalf("must follow continue token exactly once, got %v", gotContinues)
	}
	for i, lim := range gotLimits {
		if lim == "" {
			t.Fatalf("page %d must request a bounded limit, got empty", i)
		}
	}
}

// 截断可检测回归:响应超过读上限时必须显式报错,绝不把半截 body 当完整结果返回
// (裸 io.LimitReader 会返回「截断字节 + err=nil」,只留下误导性的 JSON 解码错误)。
func TestOversizedResponseFailsLoudlyInsteadOfTruncating(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// 写一份语法完整但超限的 JSON:若实现静默截断,解码会失败并掩盖真因;
		// 正确实现应在读取层就报「超过上限」。
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"continue":""},"items":[],"pad":"`))
		blob := make([]byte, agonesMaxResponseBytes+4096)
		for i := range blob {
			blob[i] = 'x'
		}
		_, _ = w.Write(blob)
		_, _ = w.Write([]byte(`"}`))
	}))
	defer srv.Close()

	a := newTestAllocator(t, srv.URL)
	_, err := a.ListAllocatedGameServers(t.Context())
	if err == nil {
		t.Fatalf("oversized response must surface an error, not a truncated success")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error must name the size limit as the cause (got %v)", err)
	}
}

func TestListAllocatedGameServersFailsClosedOnHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if _, err := a.ListAllocatedGameServers(t.Context()); err == nil {
		t.Fatalf("list must fail closed on http error")
	}
}

// deleteRecheckServer 起一个假 apiserver:GET 返回脚本化对象,记录 DELETE 请求体。
func deleteRecheckServer(t *testing.T, getStatus int, getObj map[string]any,
	deleteStatus int) (*httptest.Server, *[]byte, *int) {
	t.Helper()
	var deleteBody []byte
	deleteCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/agones.dev/v1/namespaces/pandora/gameservers/gs-x" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if getStatus != http.StatusOK {
				w.WriteHeader(getStatus)
				return
			}
			_ = json.NewEncoder(w).Encode(getObj)
		case http.MethodDelete:
			deleteCalls++
			deleteBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(deleteStatus)
			_, _ = w.Write([]byte("{}"))
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	return srv, &deleteBody, &deleteCalls
}

func allocatedGSObj(uid, allocID, state, deletionTS string) map[string]any {
	return gsListItem("gs-x", uid, "battle-fleet", allocID, state, deletionTS)
}

func TestDeleteAllocatedGameServerExactHappyPathCarriesPreconditions(t *testing.T) {
	srv, deleteBody, deleteCalls := deleteRecheckServer(t, http.StatusOK,
		allocatedGSObj("uid-x", "alloc-x", agonesStateAllocated, ""), http.StatusOK)
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)

	deleted, err := a.DeleteAllocatedGameServerExact(t.Context(), "gs-x", "uid-x", "alloc-x")
	if err != nil || !deleted {
		t.Fatalf("expected deleted=true, got deleted=%v err=%v", deleted, err)
	}
	if *deleteCalls != 1 {
		t.Fatalf("expected exactly one DELETE, got %d", *deleteCalls)
	}
	var opts deleteOptions
	if uerr := json.Unmarshal(*deleteBody, &opts); uerr != nil {
		t.Fatalf("decode delete options: %v (%s)", uerr, *deleteBody)
	}
	if opts.Preconditions == nil || opts.Preconditions.UID != "uid-x" ||
		opts.Preconditions.ResourceVersion != "100" {
		t.Fatalf("DELETE must carry uid+resourceVersion preconditions, got %+v", opts.Preconditions)
	}
}

func TestDeleteAllocatedGameServerExactRecheckMisses(t *testing.T) {
	cases := []struct {
		name      string
		getStatus int
		obj       map[string]any
	}{
		{"gone_404", http.StatusNotFound, nil},
		{"uid_changed", http.StatusOK, allocatedGSObj("uid-new", "alloc-x", agonesStateAllocated, "")},
		{"left_allocated", http.StatusOK, allocatedGSObj("uid-x", "alloc-x", "Ready", "")},
		{"allocation_id_changed", http.StatusOK, allocatedGSObj("uid-x", "alloc-new", agonesStateAllocated, "")},
		{"already_deleting", http.StatusOK, allocatedGSObj("uid-x", "alloc-x", agonesStateAllocated, "2026-08-03T00:00:00Z")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, deleteCalls := deleteRecheckServer(t, tc.getStatus, tc.obj, http.StatusOK)
			defer srv.Close()
			a := newTestAllocator(t, srv.URL)
			deleted, err := a.DeleteAllocatedGameServerExact(t.Context(), "gs-x", "uid-x", "alloc-x")
			if err != nil || deleted {
				t.Fatalf("recheck miss must be (false, nil), got deleted=%v err=%v", deleted, err)
			}
			if *deleteCalls != 0 {
				t.Fatalf("recheck miss must not DELETE, got %d calls", *deleteCalls)
			}
		})
	}
}

func TestDeleteAllocatedGameServerExactConflictIsSkipNotError(t *testing.T) {
	srv, _, deleteCalls := deleteRecheckServer(t, http.StatusOK,
		allocatedGSObj("uid-x", "alloc-x", agonesStateAllocated, ""), http.StatusConflict)
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	deleted, err := a.DeleteAllocatedGameServerExact(t.Context(), "gs-x", "uid-x", "alloc-x")
	if err != nil || deleted {
		t.Fatalf("409 precondition failure must be (false, nil), got deleted=%v err=%v", deleted, err)
	}
	if *deleteCalls != 1 {
		t.Fatalf("expected one DELETE attempt, got %d", *deleteCalls)
	}
}

func TestDeleteAllocatedGameServerExactServerErrorIsError(t *testing.T) {
	srv, _, _ := deleteRecheckServer(t, http.StatusOK,
		allocatedGSObj("uid-x", "alloc-x", agonesStateAllocated, ""), http.StatusInternalServerError)
	defer srv.Close()
	a := newTestAllocator(t, srv.URL)
	if _, err := a.DeleteAllocatedGameServerExact(t.Context(), "gs-x", "uid-x", "alloc-x"); err == nil {
		t.Fatalf("http 500 must surface as error (candidate kept for retry)")
	}
}
