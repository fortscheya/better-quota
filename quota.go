package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"time"
)

type Window struct {
	Seconds int64   `json:"seconds"`
	Used    float64 `json:"used_percent"`
	Reset   int64   `json:"reset_at"`
}
type Snapshot struct {
	Plan    string    `json:"plan"`
	Windows []Window  `json:"windows"`
	Blocked bool      `json:"blocked"`
	Fetched time.Time `json:"fetched_at"`
}
type rawWindow struct {
	Used    *float64        `json:"used_percent"`
	Seconds int64           `json:"limit_window_seconds"`
	Reset   json.RawMessage `json:"reset_at"`
	After   *int64          `json:"reset_after_seconds"`
}
type rawLimit struct {
	Allowed   *bool      `json:"allowed"`
	Reached   bool       `json:"limit_reached"`
	Primary   *rawWindow `json:"primary_window"`
	Secondary *rawWindow `json:"secondary_window"`
	Rate      *rawLimit  `json:"rate_limit"`
}

func resetAt(raw json.RawMessage, after *int64, now time.Time) (int64, error) {
	if len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		var n json.Number
		if json.Unmarshal(raw, &n) == nil {
			if v, e := strconv.ParseInt(n.String(), 10, 64); e == nil && v > 0 {
				if v > 1e12 {
					v /= 1000
				}
				return v, nil
			}
		}
		var s string
		if json.Unmarshal(raw, &s) == nil {
			if t, e := time.Parse(time.RFC3339, s); e == nil {
				return t.Unix(), nil
			}
		}
	}
	if after != nil && *after >= 0 && *after < 31536000 {
		return now.Unix() + *after, nil
	}
	return 0, errors.New("quota_reset_missing")
}

func parseUsage(raw []byte, now time.Time) (Snapshot, error) {
	var doc struct {
		Plan string                     `json:"plan_type"`
		Rate *rawLimit                  `json:"rate_limit"`
		By   map[string]json.RawMessage `json:"rate_limits_by_limit_id"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return Snapshot{}, errors.New("invalid_quota_response")
	}
	rate := doc.Rate
	// A present keyed pool is authoritative. Never borrow Spark/code-review quota.
	if doc.By != nil {
		b, ok := doc.By["codex"]
		if !ok {
			return Snapshot{}, errors.New("codex_pool_missing")
		}
		var keyed rawLimit
		if json.Unmarshal(b, &keyed) != nil {
			return Snapshot{}, errors.New("invalid_codex_pool")
		}
		rate = &keyed
		if keyed.Rate != nil {
			rate = keyed.Rate
		}
	}
	if rate == nil {
		return Snapshot{}, errors.New("codex_pool_missing")
	}
	s := Snapshot{Plan: doc.Plan, Blocked: rate.Reached || (rate.Allowed != nil && !*rate.Allowed), Fetched: now}
	seen := map[int64]bool{}
	for _, w := range []*rawWindow{rate.Primary, rate.Secondary} {
		if w == nil {
			continue
		}
		if w.Used == nil || math.IsNaN(*w.Used) || math.IsInf(*w.Used, 0) || *w.Used < 0 || *w.Used > 100 || w.Seconds <= 0 || seen[w.Seconds] {
			return Snapshot{}, errors.New("invalid_or_duplicate_quota_window")
		}
		at, e := resetAt(w.Reset, w.After, now)
		if e != nil {
			return Snapshot{}, e
		}
		seen[w.Seconds] = true
		s.Windows = append(s.Windows, Window{w.Seconds, *w.Used, at})
	}
	if len(s.Windows) == 0 {
		return Snapshot{}, errors.New("quota_windows_missing")
	}
	sort.Slice(s.Windows, func(i, j int) bool { return s.Windows[i].Seconds < s.Windows[j].Seconds })
	return s, nil
}

func quotaEligible(s Snapshot, c Config, now time.Time) string {
	if s.Fetched.IsZero() {
		return "unknown"
	}
	if now.Sub(s.Fetched) > time.Duration(c.MaxStaleSeconds)*time.Second {
		return "stale"
	}
	if s.Blocked {
		return "quota_blocked"
	}
	if len(s.Windows) == 0 {
		return "unknown"
	}
	for _, w := range s.Windows {
		if w.Reset <= now.Unix() {
			return "reset_pending"
		}
		remaining := 100 - w.Used
		if (w.Seconds == 18000 && remaining < c.Floor5H) || (w.Seconds == 604800 && remaining < c.Floor7D) || remaining <= 0 {
			return "below_floor"
		}
	}
	return ""
}

// priorityQuotaEligible checks whether the snapshot is reliable enough to
// execute a forced request. A forced target may ignore configured floors, but
// it still needs a current, non-exhausted Codex quota snapshot.
func priorityQuotaEligible(s Snapshot, c Config, now time.Time) string {
	if s.Fetched.IsZero() {
		return "unknown"
	}
	if now.Sub(s.Fetched) > time.Duration(c.MaxStaleSeconds)*time.Second {
		return "stale"
	}
	if s.Blocked {
		return "quota_blocked"
	}
	if len(s.Windows) == 0 {
		return "unknown"
	}
	for _, w := range s.Windows {
		if w.Reset <= now.Unix() {
			return "reset_pending"
		}
		if 100-w.Used <= 0 {
			return "exhausted"
		}
	}
	return ""
}

func scoreQuota(id string, s Snapshot, c Config, now time.Time) float64 {
	if quotaEligible(s, c, now) != "" {
		return 0
	}
	capacity := func(sec int64) float64 {
		if v := c.CapacityOverrides[id][sec]; v > 0 {
			return v
		}
		return 1
	}
	if c.Strategy == "capacity_weighted" {
		w := s.Windows[len(s.Windows)-1]
		return capacity(w.Seconds) * (1 - w.Used/100)
	}
	score := math.Inf(1)
	for _, w := range s.Windows {
		v := capacity(w.Seconds) * (1 - w.Used/100) / math.Max(300, float64(w.Reset-now.Unix()))
		score = math.Min(score, v)
	}
	return score
}

const maxRoutePriority = 10000

// normalizeRoutePriority mirrors the old smart-router's score/max-score
// normalization, but keeps the result in the scheduler's 0..10000 range.
// A valid, non-zero score always receives at least priority 1 so a healthy
// credential is never confused with an exhausted or unknown credential.
func normalizeRoutePriority(score, maxScore float64) (float64, int) {
	if score <= 0 || maxScore <= 0 || math.IsNaN(score) || math.IsInf(score, 0) || math.IsNaN(maxScore) || math.IsInf(maxScore, 0) {
		return 0, 0
	}
	normalized := math.Max(0, math.Min(1, score/maxScore))
	priority := int(math.Round(normalized * maxRoutePriority))
	if priority < 1 {
		priority = 1
	}
	return normalized, priority
}

func recovered(before, after Snapshot) bool {
	if before.Fetched.IsZero() || after.Fetched.IsZero() || after.Blocked || len(before.Windows) != len(after.Windows) {
		return false
	}
	improved := false
	for _, a := range before.Windows {
		found := false
		for _, b := range after.Windows {
			if a.Seconds == b.Seconds {
				found = true
				if b.Used > a.Used {
					return false
				}
				if b.Used < a.Used {
					improved = true
				}
			}
		}
		if !found {
			return false
		}
	}
	return improved
}
