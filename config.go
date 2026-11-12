package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const version = "0.1.9"
const executionHeader = "X-Better-Quota-Execution"

type AutoPingConfig struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	Mode    string   `yaml:"mode" json:"mode"`
	Times   []string `yaml:"times" json:"times"`
	Model   string   `yaml:"model" json:"model"`
}

// AutoPingOverride is a complete per-credential schedule. The absence of an
// entry means that the credential inherits Config.AutoPing.
type AutoPingOverride struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	Mode    string   `yaml:"mode" json:"mode"`
	Times   []string `yaml:"times,omitempty" json:"times,omitempty"`
}

type Config struct {
	Enabled                  bool                         `yaml:"enabled" json:"-"`
	Priority                 int                          `yaml:"priority" json:"-"`
	Store                    map[string]any               `yaml:"store" json:"-"`
	Strategy                 string                       `yaml:"strategy" json:"strategy"`
	StatePath                string                       `yaml:"state_path" json:"-"`
	FallbackProxy            string                       `yaml:"fallback_proxy_url" json:"-"`
	MaxStaleSeconds          int                          `yaml:"max_stale_seconds" json:"max_stale_seconds"`
	FetchWorkers             int                          `yaml:"fetch_workers" json:"fetch_workers"`
	Floor5H                  float64                      `yaml:"floor_5h" json:"floor_5h"`
	Floor7D                  float64                      `yaml:"floor_7d" json:"floor_7d"`
	MaxActive                int                          `yaml:"max_active" json:"max_active"`
	ActiveOverrides          map[string]int               `yaml:"active_overrides" json:"active_overrides,omitempty"`
	CapacityOverrides        map[string]map[int64]float64 `yaml:"capacity_overrides" json:"capacity_overrides,omitempty"`
	QueueLimit               int                          `yaml:"queue_limit" json:"queue_limit"`
	WaitSeconds              int                          `yaml:"wait_seconds" json:"wait_seconds"`
	ReservationSeconds       int                          `yaml:"reservation_seconds" json:"reservation_seconds"`
	StickySeconds            int                          `yaml:"sticky_seconds" json:"sticky_seconds"`
	Exclude                  []string                     `yaml:"exclude_auth_ids" json:"exclude_auth_ids,omitempty"`
	RoutePriorityOverrides   map[string]int               `yaml:"route_priority_overrides" json:"-"`
	OneShotPriorityAuthID    string                       `yaml:"-" json:"-"`
	ContinuousPriorityAuthID string                       `yaml:"-" json:"-"`
	AutoReset                struct {
		Enabled       bool     `yaml:"enabled" json:"enabled"`
		LeadSeconds   int      `yaml:"lead_seconds" json:"lead_seconds"`
		PatrolSeconds int      `yaml:"patrol_seconds" json:"patrol_seconds"`
		AuthIDs       []string `yaml:"auth_ids" json:"auth_ids,omitempty"`
	} `yaml:"auto_reset" json:"auto_reset"`
	AutoPing          AutoPingConfig              `yaml:"auto_ping" json:"auto_ping"`
	AutoPingOverrides map[string]AutoPingOverride `yaml:"auto_ping_overrides,omitempty" json:"-"`
}

func defaultConfig() Config {
	c := Config{Strategy: "capacity_weighted", StatePath: "/CLIProxyAPI/better-quota/state.json", MaxStaleSeconds: 120, FetchWorkers: 8, Floor5H: 10, Floor7D: 5, MaxActive: 3, QueueLimit: 128, WaitSeconds: 2, ReservationSeconds: 10, StickySeconds: 3600, RoutePriorityOverrides: map[string]int{}, AutoPingOverrides: map[string]AutoPingOverride{}}
	c.AutoReset.Enabled = true
	c.AutoReset.LeadSeconds = 1800
	c.AutoReset.PatrolSeconds = 300
	c.AutoPing.Mode = "daily_5h"
	c.AutoPing.Model = "gpt-6-luna"
	return c
}

func parseConfig(raw []byte) (Config, error) {
	c := defaultConfig()
	if len(raw) > 0 {
		d := yaml.NewDecoder(bytes.NewReader(raw))
		d.KnownFields(true)
		if e := d.Decode(&c); e != nil {
			return c, errors.New("invalid_config_yaml")
		}
		var extra any
		if e := d.Decode(&extra); e != io.EOF {
			return c, errors.New("multiple_config_documents_not_allowed")
		}
	}
	if c.Strategy != "capacity_weighted" && c.Strategy != "capacity_reset" {
		return c, errors.New("invalid_strategy")
	}
	var err error
	if c.AutoReset.AuthIDs, err = normalizeIDs(c.AutoReset.AuthIDs); err != nil {
		return c, err
	}
	if err := validatePriorityOverrides(c.RoutePriorityOverrides); err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.StatePath) {
		return c, errors.New("state_path_must_be_absolute")
	}
	for _, v := range []float64{c.Floor5H, c.Floor7D} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 100 {
			return c, errors.New("invalid_quota_floor")
		}
	}
	if c.MaxStaleSeconds < 30 || c.MaxStaleSeconds > 3600 || c.FetchWorkers < 1 || c.FetchWorkers > 64 || c.MaxActive < 1 || c.MaxActive > 10000 || c.QueueLimit < 0 || c.QueueLimit > 10000 || c.WaitSeconds < 0 || c.WaitSeconds > 10 || c.ReservationSeconds < 1 || c.ReservationSeconds > 60 || c.StickySeconds < 1 || c.StickySeconds > 86400 {
		return c, errors.New("invalid_limits")
	}
	for id, v := range c.ActiveOverrides {
		if id == "" || v < 1 || v > 10000 {
			return c, errors.New("invalid_active_override")
		}
	}
	for id, windows := range c.CapacityOverrides {
		for sec, v := range windows {
			if id == "" || sec <= 0 || v <= 0 || math.IsInf(v, 0) || math.IsNaN(v) {
				return c, errors.New("invalid_capacity_override")
			}
		}
	}
	if c.AutoReset.LeadSeconds < 60 || c.AutoReset.LeadSeconds > 86400 || c.AutoReset.PatrolSeconds < 30 || c.AutoReset.PatrolSeconds > 86400 {
		return c, errors.New("invalid_reset_intervals")
	}
	var e error
	if c.AutoPing, e = validateAutoPingConfig(c.AutoPing); e != nil {
		return c, e
	}
	if c.AutoPingOverrides, e = normalizeAutoPingOverrides(c.AutoPingOverrides); e != nil {
		return c, e
	}
	if c.FallbackProxy != "" {
		if _, e := newHTTPClient(c.FallbackProxy); e != nil {
			return c, e
		}
	}
	return c, nil
}

func normalizeIDs(in []string) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, id := range in {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, errors.New("invalid_auth_id")
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out, nil
}

func validatePriorityOverrides(in map[string]int) error {
	for id, priority := range in {
		if strings.TrimSpace(id) == "" || priority < -100000 || priority > 100000 {
			return errors.New("invalid_route_priority")
		}
	}
	return nil
}

func validateActiveOverrides(in map[string]int) error {
	for id, limit := range in {
		if strings.TrimSpace(id) == "" || limit < 1 || limit > 10000 {
			return errors.New("invalid_active_override")
		}
	}
	return nil
}

func cloneIDs(in []string) []string {
	if in == nil {
		return nil
	}
	return append([]string{}, in...)
}

func clonePriorityOverrides(in map[string]int) map[string]int {
	out := map[string]int{}
	for id, priority := range in {
		out[id] = priority
	}
	return out
}

func cloneActiveOverrides(in map[string]int) map[string]int {
	out := map[string]int{}
	for id, limit := range in {
		out[id] = limit
	}
	return out
}

func cloneAutoPingOverrides(in map[string]AutoPingOverride) map[string]AutoPingOverride {
	out := map[string]AutoPingOverride{}
	for id, override := range in {
		override.Times = cloneIDs(override.Times)
		out[id] = override
	}
	return out
}

func validateAutoPingConfig(in AutoPingConfig) (AutoPingConfig, error) {
	if in.Mode != "daily_5h" && in.Mode != "interval_5h" && in.Mode != "fixed_times" {
		return in, errors.New("invalid_ping_mode")
	}
	times, e := normalizeTimes(in.Times)
	if e != nil {
		return in, e
	}
	in.Times = times
	if in.Enabled && (strings.TrimSpace(in.Model) == "" || (in.Mode == "fixed_times" && len(times) == 0)) {
		return in, errors.New("ping_model_and_schedule_required")
	}
	return in, nil
}

func validateAutoPingOverride(in AutoPingOverride) (AutoPingOverride, error) {
	if in.Mode == "" {
		in.Mode = "daily_5h"
	}
	if in.Mode != "daily_5h" && in.Mode != "interval_5h" && in.Mode != "fixed_times" {
		return in, errors.New("invalid_ping_mode")
	}
	times, e := normalizeTimes(in.Times)
	if e != nil {
		return in, e
	}
	in.Times = times
	if in.Enabled && in.Mode == "fixed_times" && len(times) == 0 {
		return in, errors.New("ping_schedule_required")
	}
	return in, nil
}

func validateAutoPingOverrides(in map[string]AutoPingOverride) error {
	_, err := normalizeAutoPingOverrides(in)
	return err
}

func normalizeAutoPingOverrides(in map[string]AutoPingOverride) (map[string]AutoPingOverride, error) {
	out := map[string]AutoPingOverride{}
	for id, override := range in {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("invalid_auth_id")
		}
		clean, e := validateAutoPingOverride(override)
		if e != nil {
			return nil, e
		}
		clean.Times = cloneIDs(clean.Times)
		out[id] = clean
	}
	return out, nil
}

func applyPersistedSettings(c Config, d DiskState) (Config, error) {
	s := d.Settings
	var e error
	if s.Strategy != "" {
		if s.Strategy != "capacity_weighted" && s.Strategy != "capacity_reset" {
			return c, errors.New("state_strategy_invalid")
		}
		c.Strategy = s.Strategy
	}
	if s.AutoResetEnabled != nil {
		c.AutoReset.Enabled = *s.AutoResetEnabled
	}
	if s.AutoResetAuthIDsSet {
		ids, e := normalizeIDs(s.AutoResetAuthIDs)
		if e != nil {
			return c, errors.New("state_auth_ids_invalid")
		}
		c.AutoReset.AuthIDs = ids
	}
	if s.AutoResetPatrolSeconds != 0 {
		c.AutoReset.PatrolSeconds = s.AutoResetPatrolSeconds
	}
	if s.AutoResetLeadSeconds != 0 {
		c.AutoReset.LeadSeconds = s.AutoResetLeadSeconds
	}
	if s.MaxActive != 0 {
		if s.MaxActive < 1 || s.MaxActive > 10000 {
			return c, errors.New("state_max_active_invalid")
		}
		c.MaxActive = s.MaxActive
	}
	if s.ActiveOverridesSet {
		if e := validateActiveOverrides(s.ActiveOverrides); e != nil {
			return c, errors.New("state_active_overrides_invalid")
		}
		c.ActiveOverrides = cloneActiveOverrides(s.ActiveOverrides)
	}
	if s.RoutePriorityOverridesSet {
		if e := validatePriorityOverrides(s.RoutePriorityOverrides); e != nil {
			return c, errors.New("state_route_priority_invalid")
		}
		c.RoutePriorityOverrides = clonePriorityOverrides(s.RoutePriorityOverrides)
	}
	c.OneShotPriorityAuthID = s.OneShotPriorityAuthID
	c.ContinuousPriorityAuthID = s.ContinuousPriorityAuthID
	if s.Floor5H != nil {
		if math.IsNaN(*s.Floor5H) || math.IsInf(*s.Floor5H, 0) || *s.Floor5H < 0 || *s.Floor5H > 100 {
			return c, errors.New("state_floor_invalid")
		}
		c.Floor5H = *s.Floor5H
	}
	if s.Floor7D != nil {
		if math.IsNaN(*s.Floor7D) || math.IsInf(*s.Floor7D, 0) || *s.Floor7D < 0 || *s.Floor7D > 100 {
			return c, errors.New("state_floor_invalid")
		}
		c.Floor7D = *s.Floor7D
	}
	if s.AutoPingEnabled != nil {
		c.AutoPing.Enabled = *s.AutoPingEnabled
	}
	if s.AutoPingMode != "" {
		c.AutoPing.Mode = s.AutoPingMode
	}
	if s.AutoPingTimesSet {
		c.AutoPing.Times = cloneIDs(s.AutoPingTimes)
	}
	if c.AutoPing, e = validateAutoPingConfig(c.AutoPing); e != nil {
		return c, errors.New("state_" + e.Error())
	}
	if s.AutoPingOverridesSet {
		overrides, e := normalizeAutoPingOverrides(s.AutoPingOverrides)
		if e != nil {
			return c, errors.New("state_" + e.Error())
		}
		c.AutoPingOverrides = overrides
	}
	return c, nil
}

func autoResetSelected(c Config, id string) bool {
	if len(c.AutoReset.AuthIDs) == 0 {
		return true
	}
	for _, selected := range c.AutoReset.AuthIDs {
		if selected == id {
			return true
		}
	}
	return false
}

func normalizeTimes(in []string) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if len(s) != 5 || s[2] != ':' {
			return nil, errors.New("invalid_ping_time")
		}
		h, e := strconv.Atoi(s[:2])
		m, f := strconv.Atoi(s[3:])
		if e != nil || f != nil || h < 0 || h > 23 || m < 0 || m > 59 || fmt.Sprintf("%02d:%02d", h, m) != s {
			return nil, errors.New("invalid_ping_time")
		}
		if !seen[s] {
			out = append(out, s)
			seen[s] = true
		}
	}
	sort.Strings(out)
	return out, nil
}

var shanghai = time.FixedZone("Asia/Shanghai", 8*3600)

func pingTimes(c Config) []string {
	return pingTimesFor(c.AutoPing)
}

func pingTimesFor(c AutoPingConfig) []string {
	if c.Mode == "daily_5h" || c.Mode == "interval_5h" {
		return nil
	}
	return c.Times
}

func nextPingFor(c AutoPingConfig, now time.Time) time.Time {
	if !c.Enabled {
		return time.Time{}
	}
	local := now.In(shanghai)
	if c.Mode == "interval_5h" {
		return now.Add(5 * time.Hour)
	}
	if c.Mode == "daily_5h" {
		for day := 0; day < 2; day++ {
			for _, s := range []string{"00:00", "05:00", "10:00", "15:00", "20:00"} {
				h, _ := strconv.Atoi(s[:2])
				m, _ := strconv.Atoi(s[3:])
				t := time.Date(local.Year(), local.Month(), local.Day()+day, h, m, 0, 0, shanghai)
				if t.After(now) {
					return t
				}
			}
		}
		return time.Time{}
	}
	for day := 0; day < 2; day++ {
		for _, s := range pingTimesFor(c) {
			h, _ := strconv.Atoi(s[:2])
			m, _ := strconv.Atoi(s[3:])
			t := time.Date(local.Year(), local.Month(), local.Day()+day, h, m, 0, 0, shanghai)
			if t.After(now) {
				return t
			}
		}
	}
	return time.Time{}
}

func nextPingAfter(c AutoPingConfig, at time.Time) time.Time {
	if c.Mode == "interval_5h" {
		return at.Add(5 * time.Hour)
	}
	return nextPingFor(c, at)
}

func effectiveAutoPing(c Config, id string) AutoPingConfig {
	setting := c.AutoPing
	if override, ok := c.AutoPingOverrides[id]; ok {
		setting.Enabled = override.Enabled
		setting.Mode = override.Mode
		setting.Times = cloneIDs(override.Times)
	}
	return setting
}

func nextPing(c Config, now time.Time) time.Time {
	return nextPingFor(c.AutoPing, now)
}
