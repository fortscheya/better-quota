package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeBackend struct {
	mu            sync.Mutex
	entries       []AuthEntry
	account       string
	redeemed      bool
	posts         int
	postIDs       []string
	postCredits   []string
	dropReply     bool
	consumeStatus int
	consumeCode   string
	usageStatus   int
	creditExpiry  time.Duration
	cooldown      map[string]int
	cooldownFail  bool
	modelCalls    int
	modelCheck    func(map[string]json.RawMessage)
}

func encodeEnvelope(v any) ([]byte, error) {
	b, _ := json.Marshal(v)
	return json.Marshal(envelope{OK: true, Result: b})
}
func (f *fakeBackend) callback(method string, raw []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var in map[string]json.RawMessage
	_ = json.Unmarshal(raw, &in)
	var index string
	_ = json.Unmarshal(in["auth_index"], &index)
	switch method {
	case "host.auth.list":
		return encodeEnvelope(map[string]any{"files": f.entries})
	case "host.auth.get", "host.auth.get_runtime":
		for _, a := range f.entries {
			if a.Index == index {
				if method == "host.auth.get_runtime" {
					return encodeEnvelope(map[string]any{"auth": a})
				}
				return encodeEnvelope(map[string]any{"name": a.ID + ".json", "json": map[string]any{"type": "codex", "disabled": a.Disabled, "access_token": "synthetic-token-never-real", "account_id": f.account, "proxy_url": "direct"}})
			}
		}
		return nil, errors.New("not_found")
	case "host.routing.reset_cooldown":
		if f.cooldownFail {
			return nil, errors.New("mock_cooldown_error")
		}
		f.cooldown[index]++
		return encodeEnvelope(map[string]string{"auth_index": index})
	case "host.model.execute":
		f.modelCalls++
		if f.modelCheck != nil {
			f.modelCheck(in)
		}
		return encodeEnvelope(map[string]any{"status_code": 200, "body": []byte(`{"output":"OK"}`)})
	case "host.auth.save":
		panic("plugin must never write auth files")
	default:
		return nil, fmt.Errorf("unexpected callback %s", method)
	}
}
func (f *fakeBackend) serve(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Header.Get("Authorization") != "Bearer synthetic-token-never-real" {
		http.Error(w, "mock_auth_missing", 401)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch req.URL.Path {
	case usagePath:
		if f.usageStatus != 0 {
			w.WriteHeader(f.usageStatus)
			return
		}
		used := 90
		if f.redeemed {
			used = 0
		}
		now := time.Now().Add(time.Hour).Unix()
		_, _ = fmt.Fprintf(w, `{"plan_type":"test","rate_limits_by_limit_id":{"codex":{"primary_window":{"limit_window_seconds":18000,"used_percent":%d,"reset_at":%d},"secondary_window":{"limit_window_seconds":604800,"used_percent":%d,"reset_at":%d}}}}`, used, now, used, now+600000)
	case creditsPath:
		if f.redeemed {
			_, _ = w.Write([]byte(`{"credits":[],"available_count":0}`))
		} else {
			expiry := f.creditExpiry
			if expiry == 0 {
				expiry = 10 * time.Minute
			}
			_, _ = fmt.Fprintf(w, `{"credits":[{"id":"credit-1","status":"available","expires_at":%q}],"available_count":1}`, time.Now().Add(expiry).UTC().Format(time.RFC3339))
		}
	case consumePath:
		var body map[string]string
		if json.NewDecoder(req.Body).Decode(&body) != nil || body["credit_id"] == "" || body["redeem_request_id"] == "" || len(body) != 2 {
			http.Error(w, "invalid_mock_target", 400)
			return
		}
		f.posts++
		f.postIDs = append(f.postIDs, body["redeem_request_id"])
		f.postCredits = append(f.postCredits, body["credit_id"])
		if f.consumeStatus != 0 && f.consumeStatus != 200 {
			w.WriteHeader(f.consumeStatus)
			_, _ = w.Write([]byte(`{"error":"synthetic"}`))
			return
		}
		code := f.consumeCode
		if code == "" {
			code = "reset"
		}
		if code == "reset" || code == "already_redeemed" {
			f.redeemed = true
		}
		if f.dropReply {
			conn, _, e := w.(http.Hijacker).Hijack()
			if e == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = fmt.Fprintf(w, `{"code":%q,"windows_reset":2}`, code)
	default:
		http.NotFound(w, req)
	}
}

type backendStats struct {
	Posts        int
	IDs, Credits []string
	Cooldown     map[string]int
	Models       int
}

func (f *fakeBackend) stats() backendStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := backendStats{Posts: f.posts, IDs: append([]string{}, f.postIDs...), Credits: append([]string{}, f.postCredits...), Cooldown: map[string]int{}, Models: f.modelCalls}
	for k, v := range f.cooldown {
		s.Cooldown[k] = v
	}
	return s
}
func fixture(t *testing.T) (*Runtime, *fakeBackend, string) {
	t.Helper()
	f := &fakeBackend{account: "synthetic-account", entries: []AuthEntry{{ID: "a", Index: "idx-a", Provider: "codex"}, {ID: "alias", Index: "idx-alias", Provider: "codex"}}, cooldown: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	r := newRuntime()
	r.cfg.StatePath = filepath.Join(t.TempDir(), "state.json")
	r.baseURL = srv.URL
	r.hostCall = f.callback
	key := hashID(f.account)
	for _, a := range f.entries {
		r.accounts[a.ID] = AccountState{Auth: a, Quota: testSnapshot()}
		r.identities[a.ID] = key
	}
	r.mu.Lock()
	r.publishLocked()
	r.mu.Unlock()
	return r, f, key
}
func forceNext(t *testing.T, r *Runtime, key string) {
	t.Helper()
	i, ok := r.intent(key)
	if !ok {
		t.Fatal("intent missing")
	}
	i.Next = time.Time{}
	if e := r.saveIntent(key, i); e != nil {
		t.Fatal(e)
	}
}

func TestResetJournalBeforePOSTAndConfirmationAcrossAliases(t *testing.T) {
	r, f, key := fixture(t)
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	if f.stats().Posts != 1 {
		t.Fatal(f.stats().Posts)
	}
	i, ok := r.intent(key)
	if !ok || i.Phase != "verify" || i.RedeemID == "" || i.Before.Windows[0].Used != 90 {
		t.Fatalf("%+v", i)
	}
	b, e := os.ReadFile(r.cfg.StatePath)
	if e != nil || strings.Contains(string(b), "synthetic-token") || strings.Contains(string(b), f.account) {
		t.Fatal("journal leaked credentials or not persisted")
	}
	var d DiskState
	if json.Unmarshal(b, &d) != nil || d.Intents[key].RedeemID != f.stats().IDs[0] {
		t.Fatal("wrong durable intent")
	}
	forceNext(t, r, key)
	r.stepReset(context.Background(), key, f.entries[1], r.cfg)
	if _, ok := r.intent(key); ok {
		t.Fatal("confirmed intent not closed")
	}
	if f.stats().Posts != 1 || f.stats().Cooldown["idx-a"] != 1 || f.stats().Cooldown["idx-alias"] != 1 {
		t.Fatalf("posts %d cooldown %+v", f.stats().Posts, f.stats().Cooldown)
	}
	if len(r.disk.History) != 1 || r.accounts["a"].Quota.Windows[0].Used != 0 {
		t.Fatal("recovery not published")
	}
	st, e := os.Stat(r.cfg.StatePath)
	if e != nil || st.Mode().Perm() != 0600 {
		t.Fatal("state mode", st, e)
	}
}

func TestLostConsumeReplyRestartReconcilesWithoutSecondRedemption(t *testing.T) {
	r, f, key := fixture(t)
	f.dropReply = true
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	i, _ := r.intent(key)
	if i.Phase != "pending" || f.stats().Posts != 1 {
		t.Fatal(i, f.stats().Posts)
	}
	d, e := loadState(r.cfg.StatePath)
	if e != nil {
		t.Fatal(e)
	}
	restored := newRuntime()
	restored.cfg = r.cfg
	restored.disk = d
	restored.baseURL = r.baseURL
	restored.hostCall = f.callback
	restored.accounts = r.accounts
	restored.identities = r.identities
	forceNext(t, restored, key)
	restored.stepReset(context.Background(), key, f.entries[0], restored.cfg)
	if f.stats().Posts != 1 {
		t.Fatal("uncertain result minted another POST")
	}
	if _, ok := restored.intent(key); ok {
		t.Fatal("reconciliation incomplete")
	}
}

func TestReset5xxReusesIDAndWriteFailureNeverPosts(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		r, f, key := fixture(t)
		f.consumeStatus = 503
		r.stepReset(context.Background(), key, f.entries[0], r.cfg)
		forceNext(t, r, key)
		r.stepReset(context.Background(), key, f.entries[0], r.cfg)
		if f.stats().Posts != 2 || f.stats().IDs[0] != f.stats().IDs[1] || f.stats().Credits[0] != "credit-1" {
			t.Fatal("retry changed target or identity")
		}
	})
	t.Run("write_failure", func(t *testing.T) {
		r, f, key := fixture(t)
		r.writeState = func(string, DiskState) error { return errors.New("synthetic_disk_full") }
		r.stepReset(context.Background(), key, f.entries[0], r.cfg)
		if f.stats().Posts != 0 || r.diskFault == "" {
			t.Fatal("POST passed failed journal")
		}
	})
	t.Run("terminal_4xx", func(t *testing.T) {
		r, f, key := fixture(t)
		f.consumeStatus = 403
		r.stepReset(context.Background(), key, f.entries[0], r.cfg)
		i, _ := r.intent(key)
		if i.Phase != "manual" {
			t.Fatal(i)
		}
		r.stepReset(context.Background(), key, f.entries[0], r.cfg)
		if f.stats().Posts != 1 {
			t.Fatal("terminal response retried")
		}
	})
}

func TestCooldownRecoveryResumesWithoutConsumeAndDisabledStopsPOST(t *testing.T) {
	r, f, key := fixture(t)
	f.cooldownFail = true
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	forceNext(t, r, key)
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	i, _ := r.intent(key)
	if i.Phase != "sync" {
		t.Fatal(i)
	}
	f.cooldownFail = false
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	if f.stats().Posts != 1 {
		t.Fatal("cooldown failure caused repeat redemption")
	}
	r2, f2, k2 := fixture(t)
	f2.entries[0].Disabled = true
	r2.stepReset(context.Background(), k2, r2.accounts["a"].Auth, r2.cfg)
	if f2.stats().Posts != 0 {
		t.Fatal("disabled credential redeemed")
	}
}

func TestStateCorruptionAndIncompleteIntentFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	for _, s := range []string{`{broken`, `{"schema":1,"intents":{"x":{"phase":"pending"}},"ping_claims":{}}`, `{"schema":2,"intents":{},"ping_claims":{}}`} {
		if e := os.WriteFile(path, []byte(s), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := loadState(path); e == nil {
			t.Fatalf("accepted %s", s)
		}
	}
	d := emptyState()
	d.Intents["account"] = ResetIntent{RedeemID: uuid(), CreditID: "credit", Phase: "pending", Before: Snapshot{Fetched: time.Now()}, PreCredits: 1, Started: time.Now()}
	if e := atomicState(path, d); e != nil {
		t.Fatal(e)
	}
	if _, e := loadState(path); e == nil {
		t.Fatal("journal without quota evidence accepted")
	}
}

func TestDiscoveryCoverageAndProbeFailureRetainsLowQuota(t *testing.T) {
	r, f, _ := fixture(t)
	f.entries = append(f.entries, AuthEntry{ID: "disabled", Index: "d", Provider: "codex", Disabled: true}, AuthEntry{ID: "runtime", Index: "rt", Provider: "codex", RuntimeOnly: true}, AuthEntry{ID: "other", Index: "o", Provider: "gemini"})
	r.discover()
	if len(r.accounts) != 4 || !r.accounts["disabled"].Auth.Disabled {
		t.Fatal("discovery omitted credential types")
	}
	if _, e := r.readCredentials(r.accounts["runtime"].Auth, r.cfg); e == nil {
		t.Fatal("runtime-only silently supported")
	}
	old := r.accounts["a"]
	old.Quota.Windows[0].Used = 99
	r.accounts["a"] = old
	r.publishLocked()
	f.mu.Lock()
	f.usageStatus = 503
	f.mu.Unlock()
	r.fetchSlots <- struct{}{}
	r.jobs.Add(1)
	r.probe(context.Background(), r.accounts["a"].Auth, r.cfg)
	if a := r.accounts["a"]; a.Quota.Windows[0].Used != 99 || a.Score != 0 || a.ProbeError != "upstream_http_503" || a.Error != "" {
		t.Fatal("probe failure restored known low quota", a)
	}
	f.entries = f.entries[1:]
	r.discover()
	if _, ok := r.accounts["a"]; ok {
		t.Fatal("deleted auth retained")
	}
}

func TestPublishedRoutePriorityNormalizesScoresAndDrivesGate(t *testing.T) {
	r, _, _ := fixture(t)
	a := r.accounts["a"]
	b := r.accounts["alias"]
	b.Quota.Windows[0].Used = 60
	b.Quota.Windows[1].Used = 60
	r.accounts["a"] = a
	r.accounts["alias"] = b
	r.publishLocked()
	if r.accounts["a"].RoutePriority != 10000 || r.accounts["alias"].RoutePriority != 5000 {
		t.Fatalf("normalized priorities = %d, %d", r.accounts["a"].RoutePriority, r.accounts["alias"].RoutePriority)
	}
	mustBegin(t, r.gate, "normalized")
	p := r.gate.pick(context.Background(), pickFor("normalized", "a", "alias"))
	if p.Reject || p.AuthID != "a" {
		t.Fatalf("gate ignored normalized priority: %+v", p)
	}
	r.gate.finish("normalized")
}

func TestTransientProbeErrorKeepsFreshQuotaRoutable(t *testing.T) {
	r, f, _ := fixture(t)
	f.usageStatus = 503
	r.fetchSlots <- struct{}{}
	r.jobs.Add(1)
	r.probe(context.Background(), r.accounts["a"].Auth, r.cfg)
	a := r.accounts["a"]
	if a.Error != "" || a.ProbeError != "upstream_http_503" || a.Score <= 0 {
		t.Fatalf("transient probe state = %+v", a)
	}
	mustBegin(t, r.gate, "after-probe")
	p := r.gate.pick(context.Background(), pickFor("after-probe", "a"))
	if p.Reject {
		t.Fatalf("fresh quota was removed after transient probe error: %+v", p)
	}
	r.gate.finish("after-probe")
}

func TestPingUsesExplicitGateAndDurableScheduleClaim(t *testing.T) {
	r, f, _ := fixture(t)
	r.cfg.AutoPing.Enabled = true
	c := r.cfg
	at := time.Now().Truncate(time.Minute)
	f.modelCheck = func(in map[string]json.RawMessage) {
		var id, provider, entry, exit string
		_ = json.Unmarshal(in["auth_id"], &id)
		_ = json.Unmarshal(in["forced_provider"], &provider)
		_ = json.Unmarshal(in["entry_protocol"], &entry)
		_ = json.Unmarshal(in["exit_protocol"], &exit)
		if id != "a" || provider != "codex" || entry != "openai-response" || exit != "codex" {
			t.Errorf("wrong directed host execution")
		}
		if r.gate.stats().Admitted != 1 {
			t.Error("nested ping bypassed gate")
		}
	}
	run := func() { r.pingSlots <- struct{}{}; r.jobs.Add(1); r.pingJob(context.Background(), f.entries[0], c, at) }
	run()
	run()
	if f.stats().Models != 1 || r.gate.stats().Admitted != 0 {
		t.Fatal("duplicate ping or gate leak", f.stats().Models)
	}
	mustBegin(t, r.gate, "business")
	mustAdmit(t, r.gate, "business", "a")
	run()
	if f.stats().Models != 1 {
		t.Fatal("ping displaced live request")
	}
	r.gate.finish("business")
}

func rpcResult(t *testing.T, r *Runtime, method string, req, out any) {
	t.Helper()
	b, _ := json.Marshal(req)
	raw, e := r.dispatch(method, b)
	if e != nil {
		t.Fatal(method, e)
	}
	var env envelope
	if json.Unmarshal(raw, &env) != nil || !env.OK {
		t.Fatalf("bad rpc %s", raw)
	}
	if out != nil && json.Unmarshal(env.Result, out) != nil {
		t.Fatalf("bad result %s", raw)
	}
}
func TestRPCNoOpBodiesHeaderRemovalAndManagementAuthBoundary(t *testing.T) {
	r, _, _ := fixture(t)
	var hook HookResponse
	rpcResult(t, r, "request.intercept_before", HookRequest{RequestID: "rpc", Headers: http.Header{executionHeader: {"forged"}}}, &hook)
	if hook.Headers.Get(executionHeader) != "rpc" {
		t.Fatal("client forged context")
	}
	p := pickFor("rpc", "a", "alias")
	var pick PickResponse
	rpcResult(t, r, "scheduler.pick", p, &pick)
	rpcResult(t, r, "request.intercept_after", HookRequest{RequestID: "rpc", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": pick.AuthID}}, &hook)
	if hook.Terminate || len(hook.ClearHeaders) != 1 || hook.ClearHeaders[0] != executionHeader {
		t.Fatal("private header leaked", hook)
	}
	rpcResult(t, r, "response.intercept_stream_chunk", map[string]any{"RequestID": "rpc", "Body": []byte("data: long"), "ChunkIndex": 0}, nil)
	rpcResult(t, r, "request.complete", HookRequest{RequestID: "rpc"}, nil)
	rpcResult(t, r, "management.register", map[string]string{"BasePath": "/v0/management", "ResourceBasePath": "/v0/resource/plugins/better-quota"}, nil)
	var response managementResponse
	rpcResult(t, r, "management.handle", managementRequest{Method: "GET", Path: "/v0/resource/plugins/better-quota/dashboard"}, &response)
	if response.StatusCode != 200 || !strings.Contains(string(response.Body), "按额度和重置时间") || !strings.Contains(string(response.Body), "autoreset") {
		t.Fatal("dashboard resource missing or incomplete")
	}
	rpcResult(t, r, "management.handle", managementRequest{Method: "GET", Path: "/v0/resource/plugins/better-quota/status"}, &response)
	if response.StatusCode != 404 {
		t.Fatal("status exposed as unauthenticated resource")
	}
	rpcResult(t, r, "management.handle", managementRequest{Method: "GET", Path: "/v0/management/plugins/better-quota/status"}, &response)
	if response.StatusCode != 200 || strings.Contains(string(response.Body), "synthetic-token") {
		t.Fatal("bad status or credentials leaked")
	}
}

func TestNonCodexHooksRemainTransparentAndDoNotOccupyGate(t *testing.T) {
	r, _, _ := fixture(t)
	var hook HookResponse
	rpcResult(t, r, "request.intercept_before", HookRequest{RequestID: "non-codex"}, &hook)
	if hook.Terminate || hook.Headers.Get(executionHeader) != "non-codex" {
		t.Fatalf("before hook changed non-Codex request: %+v", hook)
	}

	var pick PickResponse
	nonCodex := PickRequest{Provider: "zcode", Model: "glm-5.3"}
	nonCodex.Candidates = []Candidate{{ID: "zcode-auth", Provider: "zcode"}}
	rpcResult(t, r, "scheduler.pick", nonCodex, &pick)
	if pick.Handled || pick.Reject {
		t.Fatalf("better-quota handled non-Codex scheduler request: %+v", pick)
	}
	rpcResult(t, r, "request.intercept_after", HookRequest{RequestID: "non-codex", ToFormat: "zcode", Metadata: map[string]any{"selected_auth_id": "zcode-auth"}}, &hook)
	if hook.Terminate || len(hook.ClearHeaders) != 1 || hook.ClearHeaders[0] != executionHeader {
		t.Fatalf("after hook changed non-Codex request: %+v", hook)
	}
	rpcResult(t, r, "response.intercept_stream_chunk", map[string]any{"RequestID": "non-codex", "Body": []byte("large-or-opaque")}, nil)
	rpcResult(t, r, "request.complete", HookRequest{RequestID: "non-codex"}, nil)
	stats := r.gate.stats()
	if stats.Admitted != 0 || stats.Reserved != 0 || len(stats.Active) != 0 || !stats.OldestActivity.IsZero() {
		t.Fatalf("non-Codex hooks polluted gate accounting: %+v", stats)
	}
}

func TestDashboardSettingsPersistAndApply(t *testing.T) {
	r, _, _ := fixture(t)
	ids := []string{"alias"}
	activeOverrides := map[string]int{"a": 3, "alias": 2}
	body, _ := json.Marshal(map[string]any{"strategy": "capacity_reset", "floor_5h": 0, "floor_7d": 7.5, "auto_reset_enabled": true, "auto_reset_auth_ids": ids, "auto_reset_patrol_seconds": 600, "auto_reset_lead_seconds": 3600, "max_active": 4, "active_overrides": activeOverrides, "auto_ping_enabled": true, "auto_ping_mode": "fixed_times", "auto_ping_times": []string{"12:00", "17:00"}, "auto_ping_overrides": map[string]any{"a": map[string]any{"enabled": true, "mode": "interval_5h"}}})
	if e := r.updateSettings(body); e != nil {
		t.Fatal(e)
	}
	if r.cfg.Strategy != "capacity_reset" || r.cfg.Floor5H != 0 || r.cfg.Floor7D != 7.5 || r.cfg.AutoReset.PatrolSeconds != 600 || r.cfg.AutoReset.LeadSeconds != 3600 || r.cfg.MaxActive != 4 || r.cfg.ActiveOverrides["a"] != 3 || !reflect.DeepEqual(r.cfg.AutoReset.AuthIDs, ids) || !r.cfg.AutoPing.Enabled || r.cfg.AutoPing.Mode != "fixed_times" || !reflect.DeepEqual(r.cfg.AutoPing.Times, []string{"12:00", "17:00"}) || r.cfg.AutoPingOverrides["a"].Mode != "interval_5h" {
		t.Fatalf("settings not applied: %+v", r.cfg)
	}
	if autoResetSelected(r.cfg, "a") || !autoResetSelected(r.cfg, "alias") {
		t.Fatal("autoreset account selection not applied")
	}
	d, e := loadState(r.cfg.StatePath)
	if e != nil || d.Settings.Strategy != "capacity_reset" || d.Settings.Floor5H == nil || *d.Settings.Floor5H != 0 || d.Settings.Floor7D == nil || *d.Settings.Floor7D != 7.5 || d.Settings.MaxActive != 4 || d.Settings.ActiveOverrides["a"] != 3 || !d.Settings.AutoResetAuthIDsSet || d.Settings.AutoPingMode != "fixed_times" || !d.Settings.AutoPingOverridesSet || d.Settings.AutoPingOverrides["a"].Mode != "interval_5h" {
		t.Fatalf("settings not persisted: %+v %v", d.Settings, e)
	}
}

func TestPingSchedulesArePerCredentialAndPartialFloorPatchPreservesSettings(t *testing.T) {
	r, _, _ := fixture(t)
	base := r.cfg
	base.AutoPing.Enabled = true
	base.AutoPing.Mode = "interval_5h"
	base.AutoPingOverrides = map[string]AutoPingOverride{
		"a":     {Enabled: true, Mode: "fixed_times", Times: []string{"12:00"}},
		"alias": {Enabled: false, Mode: "interval_5h"},
	}
	r.cfg = base
	now := time.Date(2026, 10, 26, 9, 0, 0, 0, shanghai)
	r.nextPingByAuth = map[string]time.Time{}
	r.resetPingSchedulesLocked(base, now, nil)
	if r.nextPingByAuth["a"].Format("15:04") != "12:00" {
		t.Fatalf("credential fixed time not scheduled: %+v", r.nextPingByAuth)
	}
	if !r.nextPingByAuth["alias"].IsZero() {
		t.Fatalf("disabled override still scheduled: %+v", r.nextPingByAuth)
	}
	if got := r.nextPingByAuth["a"].Sub(now); got != 3*time.Hour {
		t.Fatalf("fixed time schedule drifted by %s", got)
	}
	body, _ := json.Marshal(map[string]any{"floor_5h": 0, "floor_7d": 4.5})
	if e := r.updateSettings(body); e != nil {
		t.Fatal(e)
	}
	body, _ = json.Marshal(map[string]any{"active_overrides": map[string]int{"a": 4}})
	if e := r.updateSettings(body); e != nil {
		t.Fatal(e)
	}
	if r.cfg.Floor5H != 0 || r.cfg.Floor7D != 4.5 || r.cfg.ActiveOverrides["a"] != 4 {
		t.Fatalf("partial settings changed floor values: %+v", r.cfg)
	}
	body, _ = json.Marshal(map[string]any{"auto_ping_overrides": map[string]any{"a": nil}})
	if e := r.updateSettings(body); e != nil {
		t.Fatal(e)
	}
	if _, ok := r.cfg.AutoPingOverrides["a"]; ok {
		t.Fatal("nil ping override did not restore inheritance")
	}
}

func TestReconfigureAndQuiescePreserveLiveAccounting(t *testing.T) {
	r, _, _ := fixture(t)
	mustBegin(t, r.gate, "live")
	mustAdmit(t, r.gate, "live", "a")
	cfg := []byte("state_path: " + r.cfg.StatePath + "\nmax_active: 2\nauto_reset:\n  enabled: false\n")
	raw, _ := json.Marshal(map[string]any{"config_yaml": cfg})
	if e := r.configure(raw); e != nil {
		t.Fatal(e)
	}
	if r.gate.stats().Admitted != 1 {
		t.Fatal("reconfigure cleared active")
	}
	if _, e := r.dispatch("plugin.quiesce", []byte(`{}`)); e == nil {
		t.Fatal("quiesce accepted active unload")
	}
	if r.gate.stats().Admitted != 1 {
		t.Fatal("quiesce cleared active")
	}
	r.gate.finish("live")
	if _, e := r.dispatch("plugin.quiesce", []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
}

func TestSchedulerRejectionPreservesHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		code   string
		status int
	}{{"queue_full", 429}, {"queue_timeout", 429}, {"quota_unavailable", 503}, {"missing_execution_context", 503}, {"execution_canceled", 499}} {
		raw, e := schedulerResult(reject(tc.code))
		if e != nil {
			t.Fatal(e)
		}
		var env envelope
		if json.Unmarshal(raw, &env) != nil || env.OK || env.Error == nil || env.Error.HTTPStatus != tc.status {
			t.Fatal(string(raw))
		}
		wantRetry := 0
		if tc.status == 429 {
			wantRetry = 1
		}
		if env.Error.RetryAfterSeconds != wantRetry {
			t.Fatalf("%s retry hint = %d, want %d", tc.code, env.Error.RetryAfterSeconds, wantRetry)
		}
	}
}

func TestReconfigureCannotDiscardIntentOrSwitchJournalAfterQuiesce(t *testing.T) {
	r, f, key := fixture(t)
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	cfg := []byte("state_path: " + r.cfg.StatePath + "\nauto_reset:\n  enabled: false\n")
	raw, _ := json.Marshal(map[string]any{"config_yaml": cfg})
	if e := r.configure(raw); e != nil {
		t.Fatal(e)
	}
	r.stop()
	other, _ := json.Marshal(map[string]any{"config_yaml": []byte("state_path: " + filepath.Join(t.TempDir(), "other.json") + "\n")})
	if e := r.configure(other); e == nil {
		t.Fatal("quiesce allowed journal path switch")
	}
	if e := os.Remove(r.cfg.StatePath); e != nil {
		t.Fatal(e)
	}
	if e := r.configure(raw); e == nil {
		t.Fatal("missing durable intent silently replaced")
	}
	if _, ok := r.intent(key); !ok {
		t.Fatal("old in-memory intent discarded")
	}
}

func TestFutureCreditArmsPreciseWakeBeforePatrol(t *testing.T) {
	r, f, key := fixture(t)
	f.creditExpiry = 90 * time.Second
	c := r.cfg
	c.AutoReset.LeadSeconds = 60
	now := time.Now()
	r.stepReset(context.Background(), key, f.entries[0], c)
	if f.stats().Posts != 0 {
		t.Fatal("credit consumed before trigger window")
	}
	next := r.nextReset[key]
	if next.Before(now.Add(28*time.Second)) || next.After(now.Add(31*time.Second)) {
		t.Fatal("precise trigger not armed", next)
	}
}

func TestResetPatrolStatusReportsReadOnlySuccessAndFailure(t *testing.T) {
	r, f, key := fixture(t)
	f.creditExpiry = 24 * time.Hour
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	s := r.resetStatus[key]
	if s.CheckedAt.IsZero() || s.Available == nil || *s.Available != 1 || s.NextExpiry.IsZero() || len(s.Credits) != 1 || s.PreferredCreditID != "credit-1" || !s.Credits[0].Preferred || s.Error != "" || f.posts != 0 {
		t.Fatalf("credit patrol status: %+v, posts=%d", s, f.posts)
	}
	f.usageStatus = 503
	r.stepReset(context.Background(), key, f.entries[0], r.cfg)
	s = r.resetStatus[key]
	if s.TransientError != "upstream_http_503" || s.Error != "" || s.Available == nil || *s.Available != 1 || f.posts != 0 {
		t.Fatalf("credit patrol error not observable: %+v", s)
	}
}

func TestPingBatchCoversPoolWithBoundedWorkers(t *testing.T) {
	r, f, key := fixture(t)
	for _, id := range []string{"third", "fourth"} {
		a := AuthEntry{ID: id, Index: "idx-" + id, Provider: "codex"}
		f.entries = append(f.entries, a)
		r.accounts[id] = AccountState{Auth: a, Quota: testSnapshot()}
		r.identities[id] = key
	}
	r.cfg.AutoReset.Enabled = false
	r.cfg.AutoPing.Enabled = true
	r.lastDiscover = time.Now()
	r.timeScore = time.Now()
	r.nextPingAt = time.Now()
	for id := range r.accounts {
		r.nextFetch[id] = time.Now().Add(time.Hour)
	}
	r.publishLocked()
	original := r.hostCall
	block := make(chan struct{})
	started := make(chan struct{}, 4)
	r.hostCall = func(method string, raw []byte) ([]byte, error) {
		if method == "host.model.execute" {
			started <- struct{}{}
			<-block
		}
		return original(method, raw)
	}
	r.tick(context.Background())
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	if r.gate.stats().Admitted != 2 {
		t.Fatal("worker limit not respected")
	}
	close(block)
	r.jobs.Wait()
	if f.stats().Models != 4 || r.pingCount != 4 || r.pingSkipped != 0 || r.gate.stats().Admitted != 0 || len(r.disk.PingClaims) != 4 {
		t.Fatalf("batch did not cover all credentials: calls=%d success=%d skipped=%d", f.stats().Models, r.pingCount, r.pingSkipped)
	}
}
