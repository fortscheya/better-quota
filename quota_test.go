package main

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func windowJSON(seconds int64, used string, reset string) string {
	return fmt.Sprintf(`{"limit_window_seconds":%d,"used_percent":%s,"reset_at":%s}`, seconds, used, reset)
}
func TestUsagePoolsWindowShapesAndTimestamps(t *testing.T) {
	now := time.Unix(1800000000, 0)
	w5 := windowJSON(18000, "90", "1800003600")
	w7 := windowJSON(604800, "50", "1800604800")
	for name, body := range map[string]string{
		"legacy":       `{"rate_limit":{"primary_window":` + w5 + `,"secondary_window":` + w7 + `}}`,
		"swapped":      `{"rate_limit":{"primary_window":` + w7 + `,"secondary_window":` + w5 + `}}`,
		"keyed":        `{"rate_limit":{"primary_window":` + windowJSON(18000, "99", "1800003600") + `},"rate_limits_by_limit_id":{"codex":{"primary_window":` + w7 + `,"secondary_window":` + w5 + `},"spark":{"primary_window":` + w5 + `}}}`,
		"keyed_nested": `{"rate_limits_by_limit_id":{"codex":{"rate_limit":{"primary_window":` + w7 + `,"secondary_window":` + w5 + `}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			s, e := parseUsage([]byte(body), now)
			if e != nil {
				t.Fatal(e)
			}
			if len(s.Windows) != 2 || s.Windows[0].Seconds != 18000 || s.Windows[0].Used != 90 || s.Windows[1].Seconds != 604800 {
				t.Fatalf("%+v", s)
			}
		})
	}
	for name, reset := range map[string]string{"seconds": "1800003600", "milliseconds": "1800003600000", "rfc3339": `"2027-01-15T09:00:00Z"`} {
		t.Run(name, func(t *testing.T) {
			s, e := parseUsage([]byte(`{"rate_limit":{"primary_window":`+windowJSON(604800, "3", reset)+`}}`), now)
			if e != nil || len(s.Windows) != 1 || s.Windows[0].Seconds != 604800 || s.Windows[0].Reset != 1800003600 {
				t.Fatal(s, e)
			}
		})
	}
	s, e := parseUsage([]byte(`{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"used_percent":5,"reset_after_seconds":60}}}`), now)
	if e != nil || s.Windows[0].Reset != now.Unix()+60 {
		t.Fatal(s, e)
	}
}

func TestUsageRejectsMissingAmbiguousAndWrongPool(t *testing.T) {
	w := windowJSON(18000, "20", "1800003600")
	cases := []string{`{}`, `{"code_review_rate_limit":{"primary_window":` + w + `}}`, `{"rate_limit":{"primary_window":` + w + `},"rate_limits_by_limit_id":{"spark":{"primary_window":` + w + `}}}`, `{"rate_limit":{"primary_window":` + w + `,"secondary_window":` + w + `}}`, `{"rate_limit":{"primary_window":{"limit_window_seconds":18000,"reset_at":1800003600}}}`, `{"rate_limit":{"primary_window":` + windowJSON(18000, "101", "1800003600") + `}}`, `{"rate_limit":{"primary_window":` + windowJSON(18000, "-1", "1800003600") + `}}`, `{"rate_limit":{"primary_window":` + windowJSON(18000, "20", "0") + `}}`}
	for i, raw := range cases {
		if _, e := parseUsage([]byte(raw), time.Unix(1800000000, 0)); e == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}

func TestQuotaFloorsUnknownStaleResetAndNoInventedWindow(t *testing.T) {
	c := defaultConfig()
	now := time.Now()
	s := Snapshot{Fetched: now, Windows: []Window{{18000, 90, now.Add(time.Hour).Unix()}, {604800, 95, now.Add(time.Hour).Unix()}}}
	if e := quotaEligible(s, c, now); e != "" {
		t.Fatal("equal boundary rejected", e)
	}
	for _, i := range []int{0, 1} {
		x := s
		x.Windows = append([]Window{}, s.Windows...)
		x.Windows[i].Used += 0.0001
		if quotaEligible(x, c, now) != "below_floor" {
			t.Fatal("strict lower floor not enforced")
		}
	}
	if quotaEligible(Snapshot{}, c, now) != "unknown" {
		t.Fatal("unknown treated as full")
	}
	if quotaEligible(s, c, now.Add(121*time.Second)) != "stale" {
		t.Fatal("stale quota accepted")
	}
	s.Windows[0].Reset = now.Unix()
	if quotaEligible(s, c, now) != "reset_pending" {
		t.Fatal("reset timestamp restored quota")
	}
	raw := `{"rate_limit":{"primary_window":` + windowJSON(604800, "1", fmt.Sprint(now.Add(time.Hour).Unix())) + `}}`
	one, e := parseUsage([]byte(raw), now)
	if e != nil || len(one.Windows) != 1 {
		t.Fatal("missing window fabricated", one, e)
	}
}

func TestQuotaScoringAndRecoveryEvidence(t *testing.T) {
	now := time.Now()
	c := defaultConfig()
	s := Snapshot{Fetched: now, Windows: []Window{{18000, 50, now.Add(time.Hour).Unix()}, {604800, 20, now.Add(100 * time.Hour).Unix()}}}
	if scoreQuota("a", s, c, now) != 0.8 {
		t.Fatal("capacity score")
	}
	c.CapacityOverrides = map[string]map[int64]float64{"a": {18000: 2, 604800: 3}}
	if math.Abs(scoreQuota("a", s, c, now)-2.4) > 1e-9 {
		t.Fatal("capacity multiplier")
	}
	c.Strategy = "capacity_reset"
	want := math.Min(2*0.5/3600, 3*0.8/360000)
	if math.Abs(scoreQuota("a", s, c, now)-want) > 1e-9 {
		t.Fatal("bottleneck score")
	}
	after := s
	after.Windows = append([]Window{}, s.Windows...)
	if recovered(s, after) {
		t.Fatal("unchanged usage called recovery")
	}
	after.Windows[0].Used = 0
	if !recovered(s, after) {
		t.Fatal("real recovery missed")
	}
	after.Windows[1].Used = 30
	if recovered(s, after) {
		t.Fatal("other window worsened")
	}
	after.Windows = after.Windows[:1]
	if recovered(s, after) {
		t.Fatal("missing window counted as recovery")
	}
}

func TestNormalizeRoutePriorityUsesZeroToTenThousandRange(t *testing.T) {
	for _, tc := range []struct {
		score, max float64
		normalized float64
		priority   int
	}{
		{0, 1, 0, 0},
		{1, 0, 0, 0},
		{1, 1, 1, 10000},
		{0.25, 1, 0.25, 2500},
		{0.0001, 1, 0.0001, 1},
	} {
		n, p := normalizeRoutePriority(tc.score, tc.max)
		if math.Abs(n-tc.normalized) > 1e-12 || p != tc.priority {
			t.Fatalf("normalizeRoutePriority(%v, %v) = %v, %d; want %v, %d", tc.score, tc.max, n, p, tc.normalized, tc.priority)
		}
	}
}

func TestCreditsRejectMalformedExpiryAndOrderEarliest(t *testing.T) {
	raw := `{"available_count":2,"credits":[{"id":"later","status":"available","expires_at":"2027-01-02T00:00:00Z"},{"id":"early","status":"available","expires_at":"2027-01-01T00:00:00Z"},{"id":"never","status":"available","expires_at":null}]}`
	c, e := parseCredits([]byte(raw))
	if e != nil || c.Credits[0].ID != "early" || c.Credits[2].ID != "never" {
		t.Fatal(c, e)
	}
	for _, bad := range []string{strings.Replace(raw, "2027-01-01T00:00:00Z", "not-a-date", 1), `{"credits":[]}`, `{"credits":[],"available_count":-1}`} {
		if _, e := parseCredits([]byte(bad)); e == nil {
			t.Fatal("invalid credit accepted")
		}
	}
}
