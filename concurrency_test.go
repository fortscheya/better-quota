package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestDefaultConcurrencyThreeAndFourthAdmissionRejected(t *testing.T) {
	c := defaultConfig()
	if c.MaxActive != 3 {
		t.Fatalf("default concurrency = %d, want 3", c.MaxActive)
	}
	g := setupGate(c, "existing", "newly-imported")
	for _, id := range []string{"existing", "newly-imported"} {
		if g.stats().Credentials[id].Limit != 3 {
			t.Fatal("credential did not inherit default 3", id)
		}
	}
	for _, id := range []string{"first", "second", "third"} {
		mustBegin(t, g, id)
		mustAdmit(t, g, id, "existing")
	}
	mustBegin(t, g, "fourth")
	if e := g.admit("fourth", "existing"); e == nil {
		t.Fatal("fourth concurrent request exceeded default cap")
	}
	if g.stats().Credentials["existing"].Active != 3 {
		t.Fatal("failed admission changed existing leases")
	}
}

func TestConcurrencyPartialPatchPersistsGateAndPreservesOtherSettings(t *testing.T) {
	r, _, key := fixture(t)
	next := time.Now().Add(time.Hour)
	r.nextReset[key] = next
	if e := r.updateSettings([]byte(`{"active_overrides":{"a":5}}`)); e != nil {
		t.Fatal(e)
	}
	if r.cfg.MaxActive != 3 || !r.cfg.AutoReset.Enabled || r.nextReset[key] != next {
		t.Fatal("concurrency-only save changed defaults or autoreset scheduling")
	}
	if stats := r.gate.stats(); stats.Credentials["a"].Limit != 5 || stats.Credentials["alias"].Limit != 3 {
		t.Fatal("gate did not apply credential override immediately", stats)
	}
	d, e := loadState(r.cfg.StatePath)
	if e != nil {
		t.Fatal(e)
	}
	fresh := defaultConfig()
	fresh, e = applyPersistedSettings(fresh, d)
	if e != nil || fresh.MaxActive != 3 || fresh.ActiveOverrides["a"] != 5 {
		t.Fatal("fresh runtime lost saved override", e)
	}
	if stats := setupGate(fresh, "a", "alias").stats(); stats.Credentials["a"].Limit != 5 || stats.Credentials["alias"].Limit != 3 {
		t.Fatal("restored gate differs from persisted settings", stats)
	}
	before, _ := os.ReadFile(r.cfg.StatePath)
	if e = r.updateSettings([]byte(`{"active_overrides":{"a":7,"alias":0}}`)); e == nil {
		t.Fatal("invalid batch accepted")
	}
	after, _ := os.ReadFile(r.cfg.StatePath)
	if !bytes.Equal(before, after) || r.cfg.ActiveOverrides["a"] != 5 {
		t.Fatal("invalid batch partially changed live or persisted state")
	}
}

func TestDefaultConcurrencyHasNoVisualEditor(t *testing.T) {
	if bytes.Contains([]byte(dashboardHTML), []byte(`id="max-active"`)) {
		t.Fatal("default concurrency editor is still in dashboard")
	}
	r, _, _ := fixture(t)
	raw, _ := json.Marshal(map[string]any{"config_yaml": []byte("state_path: " + r.cfg.StatePath + "\nauto_reset:\n  enabled: false\n")})
	response, e := r.dispatch("plugin.register", raw)
	if e != nil {
		t.Fatal(e)
	}
	defer r.stop()
	if bytes.Contains(response, []byte(`"Name":"max_active"`)) {
		t.Fatal("default concurrency is exposed by Manager configuration form")
	}
}
