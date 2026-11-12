package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestDashboardEmailComesFromHostMetadata(t *testing.T) {
	r := newRuntime()
	r.hostCall = func(_ string, _ []byte) ([]byte, error) {
		return encodeEnvelope(map[string]any{"files": []map[string]any{{
			"id": "codex-internal-name-team.json", "auth_index": "internal-index",
			"provider": "codex", "email": "person@example.com",
		}}})
	}
	r.discover()
	rows := r.dashboardAccountsLocked(r.cfg, r.gate.stats().Credentials)
	if len(rows) != 1 || rows[0].Auth.Email != "person@example.com" || rows[0].Auth.ID != "codex-internal-name-team.json" {
		t.Fatal("host email or internal credential identity was lost")
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []struct {
		Credential struct{ Email string } `json:"credential"`
	}
	if json.Unmarshal(raw, &decoded) != nil || decoded[0].Credential.Email != "person@example.com" {
		t.Fatal("dashboard API lost email")
	}
}

func TestDashboardUsesQuantizedReadOnlyPriorityControls(t *testing.T) {
	r := newRuntime()
	r.accounts = map[string]AccountState{
		"a": {Auth: AuthEntry{ID: "a", Index: "a", Provider: "codex"}, Quota: testSnapshot(), Score: 1, RoutePriority: 10000},
	}
	r.cfg.OneShotPriorityAuthID = "a"
	r.cfg.ContinuousPriorityAuthID = "a"
	r.gate.update(r.cfg, r.accounts)
	rows := r.dashboardAccountsLocked(r.cfg, map[string]CredentialGateStats{"a": {Limit: 3}})
	raw, err := json.Marshal(rows)
	if err != nil || !strings.Contains(string(raw), `"quantized_score":10000`) || !strings.Contains(string(raw), `"one_shot_selected":true`) || !strings.Contains(string(raw), `"continuous_selected":true`) {
		t.Fatalf("quantized priority controls missing: %s %v", raw, err)
	}
	if strings.Contains(string(raw), "normalized_score") || strings.Contains(string(raw), `"score"`) || strings.Contains(string(raw), "route_priority") {
		t.Fatalf("raw or editable route priority leaked into dashboard JSON: %s", raw)
	}
	if strings.Contains(dashboardHTML, "route-priority") || strings.Contains(dashboardHTML, "normalized_score") || strings.Contains(dashboardHTML, "__PRIORITY_CLIENT__") {
		t.Fatal("dashboard still contains the removed priority editor")
	}
	if !strings.Contains(dashboardHTML, "priority-controls") || !strings.Contains(dashboardHTML, "优先调用（仅针对下一次请求）") || !strings.Contains(dashboardHTML, "持续优先调用") {
		t.Fatal("dashboard priority controls missing")
	}
}

func TestDashboardPriorityUsesAvailableTierThenScore(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		change func(*Runtime, map[string]CredentialGateStats)
		want   []string
	}{
		{"same tier uses score", nil, []string{"a"}},
		{"published priority tier precedes score", func(r *Runtime, _ map[string]CredentialGateStats) {
			a := r.accounts["a"]
			a.RoutePriority = 10
			r.accounts["a"] = a
			b := r.accounts["b"]
			b.RoutePriority = 20
			r.accounts["b"] = b
		}, []string{"b"}},
		{"legacy override does not replace normalized priority", func(r *Runtime, _ map[string]CredentialGateStats) {
			r.cfg.RoutePriorityOverrides = map[string]int{"b": 2}
			b := r.accounts["b"]
			b.RoutePriority = 9
			r.accounts["b"] = b
		}, []string{"a"}},
		{"cooldown skipped", func(_ *Runtime, gate map[string]CredentialGateStats) {
			gate["a"] = CredentialGateStats{Limit: 3, BlockedReason: "host_cooldown"}
		}, []string{"b"}},
		{"full slots skipped", func(_ *Runtime, gate map[string]CredentialGateStats) {
			gate["a"] = CredentialGateStats{Limit: 3, Active: 2, Reserved: 1}
		}, []string{"b"}},
		{"disabled skipped", func(r *Runtime, _ map[string]CredentialGateStats) {
			a := r.accounts["a"]
			a.Auth.Disabled = true
			r.accounts["a"] = a
		}, []string{"b"}},
		{"unknown quota skipped", func(r *Runtime, _ map[string]CredentialGateStats) {
			a := r.accounts["a"]
			a.Quota = Snapshot{}
			r.accounts["a"] = a
		}, []string{"b"}},
		{"exact ties marked", func(r *Runtime, _ map[string]CredentialGateStats) {
			a := r.accounts["b"]
			a.Score = r.accounts["a"].Score
			a.RoutePriority = r.accounts["a"].RoutePriority
			r.accounts["b"] = a
		}, []string{"a", "b"}},
		{"no available accounts", func(_ *Runtime, gate map[string]CredentialGateStats) {
			for _, id := range []string{"a", "b"} {
				gate[id] = CredentialGateStats{Limit: 3, Active: 3}
			}
		}, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			r := newRuntime()
			r.accounts = setupGate(r.cfg, "a", "b").accounts
			a := r.accounts["a"]
			a.Score = 2
			a.RoutePriority = 1000
			r.accounts["a"] = a
			b := r.accounts["b"]
			b.RoutePriority = 500
			r.accounts["b"] = b
			gate := map[string]CredentialGateStats{"a": {Limit: 3}, "b": {Limit: 3}}
			if scenario.change != nil {
				scenario.change(r, gate)
			}
			var got []string
			for _, row := range r.dashboardAccountsLocked(r.cfg, gate) {
				if row.HighestPriority {
					got = append(got, row.Auth.ID)
				}
			}
			if !reflect.DeepEqual(got, scenario.want) {
				t.Fatalf("priority indicators = %v, want %v", got, scenario.want)
			}
		})
	}
}

func TestDashboardIgnoresLegacyPriorityOverride(t *testing.T) {
	r := newRuntime()
	a := AccountState{Auth: AuthEntry{ID: "a", Index: "a", Provider: "codex", Priority: 4}, Quota: testSnapshot(), Score: 1, RoutePriority: 4}
	r.accounts = map[string]AccountState{"a": a}
	r.cfg.RoutePriorityOverrides = map[string]int{"a": 99}
	rows := r.dashboardAccountsLocked(r.cfg, map[string]CredentialGateStats{"a": {Limit: 3}})
	if len(rows) != 1 || rows[0].RoutePriority != 4 || rows[0].PriorityOverridden {
		t.Fatalf("dashboard priority = %d overridden=%v, want normalized priority 4", rows[0].RoutePriority, rows[0].PriorityOverridden)
	}
}

func TestDashboardGreenTierMatchesSchedulerPriorityTier(t *testing.T) {
	r := newRuntime()
	r.accounts = map[string]AccountState{
		"a": {Auth: AuthEntry{ID: "a", Index: "a", Provider: "codex"}, Quota: testSnapshot()},
		"b": {Auth: AuthEntry{ID: "b", Index: "b", Provider: "codex"}, Quota: testSnapshot()},
	}
	b := r.accounts["b"]
	b.Quota.Windows[0].Used = 60
	b.Quota.Windows[1].Used = 60
	r.accounts["b"] = b
	r.publishLocked()
	rows := r.dashboardAccountsLocked(r.cfg, r.gate.stats().Credentials)
	for _, row := range rows {
		if row.Auth.ID == "a" && !row.HighestPriority {
			t.Fatal("dashboard did not mark the scheduler's highest priority tier")
		}
		if row.Auth.ID == "b" && row.HighestPriority {
			t.Fatal("dashboard marked a lower priority tier")
		}
	}
	mustBegin(t, r.gate, "green-tier")
	p := r.gate.pick(context.Background(), pickFor("green-tier", "a", "b"))
	if p.Reject || p.AuthID != "a" {
		t.Fatalf("scheduler tier differs from dashboard: %+v", p)
	}
	r.gate.finish("green-tier")
}
