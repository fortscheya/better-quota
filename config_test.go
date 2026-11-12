package main

import (
	"reflect"
	"testing"
	"time"
)

func TestConfigValidationAndTimes(t *testing.T) {
	for _, raw := range []string{"unknown: true", "strategy: magic", "max_active: 0", "floor_5h: 101", "queue_limit: -1", "state_path: relative.json", "fallback_proxy_url: ftp://example.com", "auto_ping:\n  enabled: true\n  mode: fixed_times\n  times: []", "auto_ping:\n  mode: fixed_times\n  times: ['25:00']"} {
		if _, e := parseConfig([]byte(raw)); e == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	got, e := normalizeTimes([]string{"17:00", "12:00", "17:00"})
	if e != nil || !reflect.DeepEqual(got, []string{"12:00", "17:00"}) {
		t.Fatal(got, e)
	}
	for _, raw := range []string{"1:00", "24:00", "12:60", "12:3a", "12:00:00", " 12:00", "+1:00"} {
		if _, e := normalizeTimes([]string{raw}); e == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	c, e := parseConfig([]byte("floor_5h: 0\nfloor_7d: 0\n"))
	if e != nil || c.Floor5H != 0 || c.AutoPing.Enabled || !c.AutoReset.Enabled {
		t.Fatal("defaults or zero floor", c, e)
	}
	if _, e := parseConfig([]byte("enabled: true\npriority: 10\nstore: {version: 0.1.0}\n")); e != nil {
		t.Fatal("CPA's reserved config fields rejected", e)
	}
	c, e = parseConfig([]byte("route_priority_overrides: {a: 20}\nauto_reset:\n  auth_ids: [b, a, a]\n"))
	if e != nil || c.RoutePriorityOverrides["a"] != 20 || !reflect.DeepEqual(c.AutoReset.AuthIDs, []string{"a", "b"}) {
		t.Fatal("route priority or autoreset account config", c, e)
	}
	if _, e := parseConfig([]byte("route_priority_overrides: {a: 100001}")); e == nil {
		t.Fatal("out of range route priority accepted")
	}
	if _, e := parseConfig([]byte("max_active: 1\n---\nmax_active: 2\n")); e == nil {
		t.Fatal("multiple config documents accepted")
	}
}

func TestPingUTC8DailyReanchorFixedTimesAndNoStartupCatchup(t *testing.T) {
	c := defaultConfig()
	c.AutoPing.Enabled = true
	for _, tc := range []struct{ at, want string }{{"2026-10-24T19:59:59+08:00", "2026-10-24T20:00:00+08:00"}, {"2026-10-24T20:00:00+08:00", "2026-10-25T00:00:00+08:00"}, {"2026-10-24T00:00:30+08:00", "2026-10-24T05:00:00+08:00"}, {"2026-10-24T15:59:59Z", "2026-10-25T00:00:00+08:00"}} {
		now, _ := time.Parse(time.RFC3339, tc.at)
		if got := nextPing(c, now).Format(time.RFC3339); got != tc.want {
			t.Fatal(tc.at, got, tc.want)
		}
	}
	c.AutoPing.Mode = "fixed_times"
	c.AutoPing.Times = []string{"12:00", "17:00"}
	now, _ := time.Parse(time.RFC3339, "2026-10-24T17:00:00+08:00")
	if got := nextPing(c, now).Format(time.RFC3339); got != "2026-10-25T12:00:00+08:00" {
		t.Fatal(got)
	}
	c.AutoPing.Enabled = false
	if !nextPing(c, now).IsZero() {
		t.Fatal("disabled ping scheduled")
	}
	interval := AutoPingConfig{Enabled: true, Mode: "interval_5h", Model: "gpt-6-luna"}
	first := nextPingFor(interval, now)
	if !first.Equal(now.Add(5*time.Hour)) || !nextPingAfter(interval, first).Equal(first.Add(5*time.Hour)) {
		t.Fatalf("strict five-hour schedule drifted: first=%s next=%s", first, nextPingAfter(interval, first))
	}
	override := Config{AutoPing: c.AutoPing, AutoPingOverrides: map[string]AutoPingOverride{"a": {Enabled: true, Mode: "fixed_times", Times: []string{"12:00"}}}}
	if got := effectiveAutoPing(override, "a"); got.Mode != "fixed_times" || !reflect.DeepEqual(got.Times, []string{"12:00"}) {
		t.Fatalf("credential schedule not applied: %+v", got)
	}
	if got := effectiveAutoPing(override, "missing"); got.Mode != c.AutoPing.Mode {
		t.Fatalf("global schedule inheritance changed: %+v", got)
	}
}
