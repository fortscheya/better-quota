package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testSnapshot() Snapshot {
	return Snapshot{Plan: "test", Fetched: time.Now(), Windows: []Window{{18000, 20, time.Now().Add(4 * time.Hour).Unix()}, {604800, 20, time.Now().Add(6 * 24 * time.Hour).Unix()}}}
}
func setupGate(c Config, ids ...string) *Gate {
	g := newGate(c)
	a := map[string]AccountState{}
	for _, id := range ids {
		a[id] = AccountState{Auth: AuthEntry{ID: id, Index: id, Provider: "codex"}, Quota: testSnapshot(), Score: 1, RoutePriority: 1000}
	}
	g.update(c, a)
	return g
}
func pickFor(id string, ids ...string) PickRequest {
	r := PickRequest{Provider: "codex", Model: "test"}
	r.Options.Headers = map[string][]string{executionHeader: {id}}
	r.Options.Metadata = map[string]any{}
	for _, a := range ids {
		r.Candidates = append(r.Candidates, Candidate{ID: a, Provider: "codex"})
	}
	return r
}
func mustBegin(t *testing.T, g *Gate, id string) {
	t.Helper()
	if e := g.begin(id); e != nil {
		t.Fatal(e)
	}
}
func mustAdmit(t *testing.T, g *Gate, id, a string) {
	t.Helper()
	if e := g.admit(id, a); e != nil {
		t.Fatal(e)
	}
}
func waitQueued(t *testing.T, g *Gate, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if g.stats().Queued == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued=%d want %d", g.stats().Queued, n)
}

func TestSyntheticExecutorConcurrencyAndTerminalRelease(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 2
	c.QueueLimit = 1000
	c.WaitSeconds = 10
	g := setupGate(c, "a", "b", "c")
	var actual [3]atomic.Int32
	var maxSeen [3]atomic.Int32
	var total atomic.Int32
	var peak atomic.Int32
	var wg sync.WaitGroup
	fail := make(chan string, 100)
	for i := 0; i < 120; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("req-%d", i)
			if e := g.begin(id); e != nil {
				fail <- e.Error()
				return
			}
			defer g.finish(id)
			p := g.pick(context.Background(), pickFor(id, "a", "b", "c"))
			if p.Reject {
				fail <- p.RejectCode
				return
			}
			if e := g.admit(id, p.AuthID); e != nil {
				fail <- e.Error()
				return
			}
			index := int(p.AuthID[0] - 'a')
			n := actual[index].Add(1)
			for m := maxSeen[index].Load(); n > m && !maxSeen[index].CompareAndSwap(m, n); m = maxSeen[index].Load() {
			}
			all := total.Add(1)
			for m := peak.Load(); all > m && !peak.CompareAndSwap(m, all); m = peak.Load() {
			}
			if n > 2 {
				fail <- "real executor exceeded credential limit"
			}
			// Synthetic SSE/WS events and an idle interval. Only a real terminal end releases.
			g.touch(id)
			time.Sleep(time.Duration(1+i%4) * time.Millisecond)
			g.touch(id)
			actual[index].Add(-1)
			total.Add(-1)
			g.finish(id)
		}(i)
	}
	wg.Wait()
	close(fail)
	for e := range fail {
		t.Error(e)
	}
	s := g.stats()
	if s.Admitted != 0 || s.Reserved != 0 || s.Queued != 0 || len(s.Active) != 0 {
		t.Fatalf("leaked %+v", s)
	}
	for i := range maxSeen {
		if maxSeen[i].Load() != 2 {
			t.Fatalf("executor %d peak %d", i, maxSeen[i].Load())
		}
	}
	if peak.Load() < 4 {
		t.Fatalf("idle accounts not used, peak=%d", peak.Load())
	}
}

func TestQueueFullCancelTimeoutAndOtherCredential(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1 // This scenario deliberately fills a one-slot credential.
	c.QueueLimit = 1
	c.WaitSeconds = 1
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "active")
	mustAdmit(t, g, "active", "a")
	mustBegin(t, g, "queued")
	result := make(chan PickResponse, 1)
	go func() { result <- g.pick(context.Background(), pickFor("queued", "a")) }()
	waitQueued(t, g, 1)
	mustBegin(t, g, "full")
	if p := g.pick(context.Background(), pickFor("full", "a")); p.RejectCode != "queue_full" {
		t.Fatalf("%+v", p)
	}
	mustBegin(t, g, "free")
	if p := g.pick(context.Background(), pickFor("free", "a", "b")); p.AuthID != "b" {
		t.Fatalf("unused account skipped: %+v", p)
	}
	g.finish("free")
	g.finish("queued")
	if p := <-result; p.RejectCode != "execution_canceled" {
		t.Fatalf("%+v", p)
	}
	mustBegin(t, g, "timeout")
	if p := g.pick(context.Background(), pickFor("timeout", "a")); p.RejectCode != "queue_timeout" {
		t.Fatalf("%+v", p)
	}
	g.finish("timeout")
	g.finish("full")
	g.finish("active")
	if len(g.stats().Active) != 0 {
		t.Fatal("leak")
	}
}

func TestNormalizedRoutePriorityWinsBeforeCandidatePriority(t *testing.T) {
	c := defaultConfig()
	g := setupGate(c, "a", "b")
	a := g.accounts["a"]
	a.RoutePriority = 20
	b := g.accounts["b"]
	b.RoutePriority = 10
	g.update(c, map[string]AccountState{"a": a, "b": b})
	mustBegin(t, g, "priority")
	req := pickFor("priority", "a", "b")
	req.Candidates[0].Priority = 1
	req.Candidates[1].Priority = 100
	p := g.pick(context.Background(), req)
	if p.Reject || p.AuthID != "a" {
		t.Fatalf("priority override was ignored: %+v", p)
	}
	g.finish("priority")
}

func TestCredentialConcurrencyStatsSeparateActiveReservedAndBlocked(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 2
	g := setupGate(c, "a")
	mustBegin(t, g, "reserved")
	if p := g.pick(context.Background(), pickFor("reserved", "a")); p.AuthID != "a" {
		t.Fatal(p)
	}
	mustBegin(t, g, "active")
	mustAdmit(t, g, "active", "a")
	s := g.stats()
	if got := s.Credentials["a"]; got.Limit != 2 || got.Active != 1 || got.Reserved != 1 || got.BlockedReason != "" {
		t.Fatalf("unexpected credential stats: %+v", got)
	}
	g.finish("reserved")
	g.finish("active")
}

func TestLongAndSilentExecutionNeverTTLReclaimed(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1 // This scenario deliberately fills a one-slot credential.
	c.WaitSeconds = 0
	g := setupGate(c, "a")
	now := time.Now()
	g.now = func() time.Time { return now }
	mustBegin(t, g, "long")
	mustAdmit(t, g, "long", "a")
	now = now.Add(25 * time.Hour)
	g.mu.Lock()
	a := g.accounts["a"]
	a.Quota = testSnapshot()
	a.Quota.Fetched = now
	for i := range a.Quota.Windows {
		a.Quota.Windows[i].Reset = now.Add(7 * 24 * time.Hour).Unix()
	}
	g.accounts["a"] = a
	g.mu.Unlock()
	mustBegin(t, g, "next")
	if p := g.pick(context.Background(), pickFor("next", "a")); !p.Reject {
		t.Fatal("live silent request was reclaimed")
	}
	g.touch("long")
	if g.stats().Admitted != 1 {
		t.Fatal("lost live request")
	}
	g.finish("long")
	g.finish("long")
	if len(g.stats().Active) != 0 {
		t.Fatal("double release changed another slot")
	}
}

func TestExpiredReservationMustReadmitAndCannotOverbook(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1 // This scenario deliberately fills a one-slot credential.
	g := setupGate(c, "a")
	now := time.Now()
	g.now = func() time.Time { return now }
	mustBegin(t, g, "late")
	if p := g.pick(context.Background(), pickFor("late", "a")); p.AuthID != "a" {
		t.Fatal(p)
	}
	now = now.Add(11 * time.Second)
	mustBegin(t, g, "other")
	mustAdmit(t, g, "other", "a")
	if e := g.admit("late", "a"); e == nil {
		t.Fatal("expired reservation bypassed cap")
	}
	g.finish("late")
	if g.stats().Active["a"] != 1 {
		t.Fatal("late release affected current lease")
	}
	g.finish("other")
}

func TestRetriesIdempotentCrossModelAndAuthHandoff(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "r")
	p := g.pick(context.Background(), pickFor("r", "a"))
	if p.AuthID != "a" {
		t.Fatal(p)
	}
	mustAdmit(t, g, "r", "a")
	mustAdmit(t, g, "r", "a")
	if s := g.stats(); s.Active["a"] != 1 || s.Admitted != 1 {
		t.Fatal(s)
	}
	mustBegin(t, g, "other-model")
	req := pickFor("other-model", "a", "b")
	req.Model = "different"
	if p := g.pick(context.Background(), req); p.AuthID != "b" {
		t.Fatal("gate incorrectly scoped by model", p)
	}
	g.finish("other-model")
	p = g.pick(context.Background(), pickFor("r", "b"))
	if p.AuthID != "b" {
		t.Fatal(p)
	}
	mustAdmit(t, g, "r", "b")
	s := g.stats()
	if s.Active["a"] != 0 || s.Active["b"] != 1 {
		t.Fatal(s)
	}
	g.finish("r")
	g.finish("r")
	if len(g.stats().Active) != 0 {
		t.Fatal("leak")
	}
}

func TestStickyParentMigrationPriorityAndCallerIsolation(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1 // This scenario deliberately fills a one-slot credential.
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "root")
	r := pickFor("root", "a", "b")
	r.Options.Metadata = map[string]any{"canonical_session_id": "root", "caller_scope": "caller"}
	p := g.pick(context.Background(), r)
	g.finish("root")
	mustBegin(t, g, "child")
	r = pickFor("child", "a", "b")
	r.Options.Metadata = map[string]any{"canonical_session_id": "child", "parent_session_id": "root", "caller_scope": "caller"}
	child := g.pick(context.Background(), r)
	if child.AuthID != p.AuthID {
		t.Fatal("did not inherit parent")
	}
	mustAdmit(t, g, "child", child.AuthID)
	mustBegin(t, g, "migrate")
	r = pickFor("migrate", "a", "b")
	r.Options.Metadata = map[string]any{"canonical_session_id": "child", "caller_scope": "caller"}
	m := g.pick(context.Background(), r)
	if m.AuthID == child.AuthID || m.Reject {
		t.Fatal("full binding not migrated", m)
	}
	g.finish("migrate")
	g.finish("child")
	if scopeKey(r, "same") == scopeKey(PickRequest{Model: r.Model}, "same") {
		t.Fatal("caller scope not isolated")
	}
	mustBegin(t, g, "priority")
	r = pickFor("priority", "a", "b")
	r.Candidates[1].Priority = 10
	b := g.accounts["b"]
	b.RoutePriority = 1000
	a := g.accounts["a"]
	a.RoutePriority = 500
	g.update(c, map[string]AccountState{"a": a, "b": b})
	if p := g.pick(context.Background(), r); p.AuthID != "a" {
		t.Fatal("priority ignored", p)
	}
	g.finish("priority")
}

func TestMissingContextNonCodexAndDraining(t *testing.T) {
	g := setupGate(defaultConfig(), "a")
	if p := g.pick(context.Background(), pickFor("forged", "a")); p.RejectCode != "missing_execution_context" {
		t.Fatal(p)
	}
	if p := g.pick(context.Background(), PickRequest{Provider: "gemini"}); p.Handled {
		t.Fatal("non-codex overridden")
	}
	mustBegin(t, g, "active")
	mustAdmit(t, g, "active", "a")
	g.drain()
	if g.stats().Admitted != 1 {
		t.Fatal("drain cleared active")
	}
	mustBegin(t, g, "new")
	if e := g.admit("new", "a"); e == nil {
		t.Fatal("drain admitted new Codex request")
	}
	if p := g.pick(context.Background(), PickRequest{Provider: "gemini"}); p.Handled {
		t.Fatal("drain intercepted other providers")
	}
	g.finish("active")
	g.resume()
	if e := g.begin("active"); e == nil {
		t.Fatal("completed ID reused")
	}
}

func TestTopThreeUtilizationAndQuotaRecoveryCannotInventCapacity(t *testing.T) {
	c := defaultConfig()
	g := setupGate(c, "a", "b", "c", "d")
	for id, priority := range map[string]int{"a": 10000, "b": 9000, "c": 8000, "d": 1000} {
		a := g.accounts[id]
		a.RoutePriority = priority
		g.accounts[id] = a
	}
	// The fourth account is excluded before utilization selection even though
	// it is otherwise healthy.
	mustBegin(t, g, "top-three")
	if p := g.pick(context.Background(), pickFor("top-three", "a", "b", "c", "d")); p.AuthID != "a" {
		t.Fatalf("top score was not selected: %+v", p)
	}
	g.finish("top-three")
	mustBegin(t, g, "reserve-a")
	mustAdmit(t, g, "reserve-a", "a")
	mustBegin(t, g, "utilization")
	if p := g.pick(context.Background(), pickFor("utilization", "a", "b", "c", "d")); p.AuthID != "b" {
		t.Fatalf("lowest utilization in top three was not selected: %+v", p)
	}
	g.finish("utilization")
	g.finish("reserve-a")
	a := g.accounts["a"]
	a.Quota.Windows[0].Used = 99
	g.accounts["a"] = a
	mustBegin(t, g, "floor")
	if p := g.pick(context.Background(), pickFor("floor", "a", "b")); p.AuthID != "b" {
		t.Fatal("hard quota floor ignored", p)
	}
	g.finish("floor")
	a.Quota.Windows[0].Reset = time.Now().Add(-time.Second).Unix()
	g.accounts["a"] = a
	mustBegin(t, g, "elapsed")
	if p := g.pick(context.Background(), pickFor("elapsed", "a")); p.RejectCode != "quota_unavailable" {
		t.Fatal("elapsed reset invented recovery", p)
	}
}

func TestNestedHostPingReusesOnlyItsExactPreAdmittedSlot(t *testing.T) {
	g := setupGate(defaultConfig(), "a", "b")
	mustBegin(t, g, "ping-internal")
	mustAdmit(t, g, "ping-internal", "a")
	if !g.pinInternal("ping-internal", "a") {
		t.Fatal("internal pin failed")
	}
	req := pickFor("", "a")
	req.Options.Headers = nil
	req.Options.Metadata = map[string]any{"source": "plugin_host_model_callback", "pinned_auth_id": "a"}
	if p := g.pick(context.Background(), req); p.AuthID != "a" || p.Reject {
		t.Fatal("nested execution double-counted", p)
	}
	if g.stats().Admitted != 1 {
		t.Fatal("nested request acquired another slot")
	}
	req.Options.Metadata["pinned_auth_id"] = "b"
	if p := g.pick(context.Background(), req); !p.Reject {
		t.Fatal("unrelated pinned execution borrowed slot")
	}
	g.finish("ping-internal")
	req.Options.Metadata["pinned_auth_id"] = "a"
	if p := g.pick(context.Background(), req); !p.Reject {
		t.Fatal("released internal pin reused")
	}
}

func TestStickyBindingSurvivesQuantizedPriorityChangesUntilFull(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1
	g := setupGate(c, "a", "b")
	a := g.accounts["a"]
	a.RoutePriority = 10000
	b := g.accounts["b"]
	b.RoutePriority = 5000
	g.update(c, map[string]AccountState{"a": a, "b": b})
	first := pickFor("sticky-root", "a", "b")
	first.Options.Metadata = map[string]any{"canonical_session_id": "session", "caller_scope": "caller"}
	mustBegin(t, g, "sticky-root")
	if p := g.pick(context.Background(), first); p.AuthID != "a" {
		t.Fatal(p)
	}
	g.finish("sticky-root")
	a.RoutePriority = 1
	b.RoutePriority = 10000
	g.update(c, map[string]AccountState{"a": a, "b": b})
	mustBegin(t, g, "sticky-followup")
	followup := first
	followup.Options.Headers = map[string][]string{executionHeader: {"sticky-followup"}}
	if p := g.pick(context.Background(), followup); p.AuthID != "a" {
		t.Fatalf("sticky binding lost after score change: %+v", p)
	}
	g.finish("sticky-followup")
	mustBegin(t, g, "fill-a")
	fill := pickFor("fill-a", "a", "b")
	fill.Options.Metadata = map[string]any{"canonical_session_id": "session", "caller_scope": "caller"}
	if p := g.pick(context.Background(), fill); p.AuthID != "a" {
		t.Fatal(p)
	}
	mustAdmit(t, g, "fill-a", "a")
	mustBegin(t, g, "migrated")
	migrated := first
	migrated.Options.Headers = map[string][]string{executionHeader: {"migrated"}}
	if p := g.pick(context.Background(), migrated); p.AuthID != "b" {
		t.Fatalf("full sticky binding did not migrate: %+v", p)
	}
	g.finish("migrated")
	g.finish("fill-a")
}

func TestPriorityTargetOverridesFloorAndConcurrencyWithoutStickyPollution(t *testing.T) {
	c := defaultConfig()
	c.MaxActive = 1
	c.OneShotPriorityAuthID = "a"
	c.ContinuousPriorityAuthID = "b"
	g := setupGate(c, "a", "b")
	a := g.accounts["a"]
	a.Quota.Windows[0].Used = 95
	a.Quota.Windows[1].Used = 95
	a.RoutePriority = 1
	g.accounts["a"] = a
	g.update(c, g.accounts)
	mustBegin(t, g, "forced")
	if p := g.pick(context.Background(), pickFor("forced", "a", "b")); p.AuthID != "a" {
		t.Fatalf("one-shot target did not override ordinary policy: %+v", p)
	}
	mustAdmit(t, g, "forced", "a")
	if got := g.stats().Credentials["a"]; got.Active != 1 || got.Limit != 1 {
		t.Fatalf("forced execution was not counted: %+v", got)
	}
	if !g.consumeOneShot("forced", "a") {
		t.Fatal("one-shot target was not consumed after after-auth")
	}
	g.finish("forced")
	mustBegin(t, g, "continuous")
	if p := g.pick(context.Background(), pickFor("continuous", "a", "b")); p.AuthID != "b" {
		t.Fatalf("continuous target did not resume after one-shot: %+v", p)
	}
	mustAdmit(t, g, "continuous", "b")
	g.finish("continuous")
	if len(g.sticky) != 0 {
		t.Fatal("priority request polluted sticky bindings")
	}
}

func TestPriorityTargetHardUnavailableDoesNotFallback(t *testing.T) {
	c := defaultConfig()
	c.OneShotPriorityAuthID = "a"
	g := setupGate(c, "a", "b")
	a := g.accounts["a"]
	a.Auth.Disabled = true
	g.accounts["a"] = a
	g.update(c, g.accounts)
	mustBegin(t, g, "disabled-target")
	if p := g.pick(context.Background(), pickFor("disabled-target", "a", "b")); p.RejectCode != "priority_target_unavailable" {
		t.Fatalf("disabled priority target fell back: %+v", p)
	}
	g.finish("disabled-target")
	a.Auth.Disabled = false
	a.Error = "upstream_http_401"
	g.accounts["a"] = a
	g.update(c, g.accounts)
	mustBegin(t, g, "invalid-target")
	if p := g.pick(context.Background(), pickFor("invalid-target", "a", "b")); p.RejectCode != "priority_target_unavailable" {
		t.Fatalf("invalid priority target fell back: %+v", p)
	}
	g.finish("invalid-target")
	a.Error = ""
	a.Auth.Unavailable = true
	a.Auth.NextRetryAfter = time.Now().Add(time.Minute)
	g.accounts["a"] = a
	g.update(c, g.accounts)
	mustBegin(t, g, "cooldown-target")
	if p := g.pick(context.Background(), pickFor("cooldown-target", "a", "b")); p.RejectCode != "priority_target_unavailable" {
		t.Fatalf("cooldown priority target fell back: %+v", p)
	}
	g.finish("cooldown-target")
}

func TestPriorityLeaseRechecksHardAvailabilityOnIdempotentAdmit(t *testing.T) {
	c := defaultConfig()
	c.OneShotPriorityAuthID = "a"
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "forced-idempotent")
	if p := g.pick(context.Background(), pickFor("forced-idempotent", "a", "b")); p.AuthID != "a" {
		t.Fatal(p)
	}
	mustAdmit(t, g, "forced-idempotent", "a")
	a := g.accounts["a"]
	a.Auth.Disabled = true
	g.accounts["a"] = a
	g.update(c, g.accounts)
	if err := g.admit("forced-idempotent", "a"); err == nil || err.Error() != "priority_target_unavailable" {
		t.Fatalf("idempotent admit bypassed hard availability: %v", err)
	}
	g.finish("forced-idempotent")
}
