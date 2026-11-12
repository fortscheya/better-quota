package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ResetIntent struct {
	RedeemID   string          `json:"redeem_request_id"`
	CreditID   string          `json:"credit_id"`
	Phase      string          `json:"phase"`
	Before     Snapshot        `json:"before"`
	PreCredits int             `json:"pre_credits"`
	Started    time.Time       `json:"started_at"`
	Next       time.Time       `json:"next_at"`
	Attempts   int             `json:"attempts"`
	LastError  string          `json:"last_error,omitempty"`
	Synced     map[string]bool `json:"synced,omitempty"`
}
type ResetRecord struct {
	Account  string    `json:"account"`
	CreditID string    `json:"credit_id"`
	At       time.Time `json:"at"`
}
type PersistedSettings struct {
	Strategy                  string                      `json:"strategy,omitempty"`
	Floor5H                   *float64                    `json:"floor_5h,omitempty"`
	Floor7D                   *float64                    `json:"floor_7d,omitempty"`
	AutoResetEnabled          *bool                       `json:"auto_reset_enabled,omitempty"`
	AutoResetAuthIDs          []string                    `json:"auto_reset_auth_ids,omitempty"`
	AutoResetAuthIDsSet       bool                        `json:"auto_reset_auth_ids_set,omitempty"`
	AutoResetPatrolSeconds    int                         `json:"auto_reset_patrol_seconds,omitempty"`
	AutoResetLeadSeconds      int                         `json:"auto_reset_lead_seconds,omitempty"`
	MaxActive                 int                         `json:"max_active,omitempty"`
	ActiveOverrides           map[string]int              `json:"active_overrides,omitempty"`
	ActiveOverridesSet        bool                        `json:"active_overrides_set,omitempty"`
	RoutePriorityOverrides    map[string]int              `json:"route_priority_overrides,omitempty"`
	RoutePriorityOverridesSet bool                        `json:"route_priority_overrides_set,omitempty"`
	OneShotPriorityAuthID     string                      `json:"one_shot_priority_auth_id,omitempty"`
	ContinuousPriorityAuthID  string                      `json:"continuous_priority_auth_id,omitempty"`
	AutoPingEnabled           *bool                       `json:"auto_ping_enabled,omitempty"`
	AutoPingMode              string                      `json:"auto_ping_mode,omitempty"`
	AutoPingTimes             []string                    `json:"auto_ping_times,omitempty"`
	AutoPingTimesSet          bool                        `json:"auto_ping_times_set,omitempty"`
	AutoPingOverrides         map[string]AutoPingOverride `json:"auto_ping_overrides,omitempty"`
	AutoPingOverridesSet      bool                        `json:"auto_ping_overrides_set,omitempty"`
}
type DiskState struct {
	Schema     int                    `json:"schema"`
	Intents    map[string]ResetIntent `json:"intents"`
	History    []ResetRecord          `json:"history"`
	PingClaims map[string]time.Time   `json:"ping_claims"`
	Settings   PersistedSettings      `json:"settings,omitempty"`
}

func emptyState() DiskState {
	return DiskState{Schema: 1, Intents: map[string]ResetIntent{}, PingClaims: map[string]time.Time{}}
}
func loadState(path string) (DiskState, error) {
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		return emptyState(), nil
	}
	if e != nil {
		return DiskState{}, errors.New("state_read_failed")
	}
	var d DiskState
	if json.Unmarshal(b, &d) != nil || d.Schema != 1 || d.Intents == nil || d.PingClaims == nil {
		return d, errors.New("state_invalid_fail_closed")
	}
	for key, i := range d.Intents {
		if key == "" || i.RedeemID == "" || i.CreditID == "" || i.Before.Fetched.IsZero() || len(i.Before.Windows) == 0 || i.PreCredits < 1 || i.Attempts < 0 || i.Started.IsZero() {
			return d, errors.New("state_intent_invalid")
		}
		seen := map[int64]bool{}
		for _, w := range i.Before.Windows {
			if w.Seconds <= 0 || w.Used < 0 || w.Used > 100 || w.Reset <= 0 || seen[w.Seconds] {
				return d, errors.New("state_snapshot_invalid")
			}
			seen[w.Seconds] = true
		}
		if i.Phase != "pending" && i.Phase != "verify" && i.Phase != "sync" && i.Phase != "manual" {
			return d, errors.New("state_phase_invalid")
		}
	}
	if d.Settings.Strategy != "" && d.Settings.Strategy != "capacity_weighted" && d.Settings.Strategy != "capacity_reset" {
		return d, errors.New("state_strategy_invalid")
	}
	if _, e := normalizeIDs(d.Settings.AutoResetAuthIDs); e != nil {
		return d, errors.New("state_auth_ids_invalid")
	}
	if d.Settings.AutoResetPatrolSeconds != 0 && (d.Settings.AutoResetPatrolSeconds < 30 || d.Settings.AutoResetPatrolSeconds > 86400) {
		return d, errors.New("state_reset_patrol_invalid")
	}
	if d.Settings.AutoResetLeadSeconds != 0 && (d.Settings.AutoResetLeadSeconds < 60 || d.Settings.AutoResetLeadSeconds > 86400) {
		return d, errors.New("state_reset_lead_invalid")
	}
	if d.Settings.MaxActive != 0 && (d.Settings.MaxActive < 1 || d.Settings.MaxActive > 10000) {
		return d, errors.New("state_max_active_invalid")
	}
	if e := validateActiveOverrides(d.Settings.ActiveOverrides); e != nil {
		return d, errors.New("state_active_overrides_invalid")
	}
	if e := validatePriorityOverrides(d.Settings.RoutePriorityOverrides); e != nil {
		return d, errors.New("state_route_priority_invalid")
	}
	for _, floor := range []*float64{d.Settings.Floor5H, d.Settings.Floor7D} {
		if floor != nil && (math.IsNaN(*floor) || math.IsInf(*floor, 0) || *floor < 0 || *floor > 100) {
			return d, errors.New("state_floor_invalid")
		}
	}
	if d.Settings.AutoPingTimesSet {
		if _, e := normalizeTimes(d.Settings.AutoPingTimes); e != nil {
			return d, errors.New("state_ping_schedule_invalid")
		}
	}
	if d.Settings.AutoPingMode != "" && d.Settings.AutoPingMode != "daily_5h" && d.Settings.AutoPingMode != "interval_5h" && d.Settings.AutoPingMode != "fixed_times" {
		return d, errors.New("state_ping_mode_invalid")
	}
	for _, id := range []string{d.Settings.OneShotPriorityAuthID, d.Settings.ContinuousPriorityAuthID} {
		if strings.TrimSpace(id) != id {
			return d, errors.New("state_priority_auth_id_invalid")
		}
	}
	if e := validateAutoPingOverrides(d.Settings.AutoPingOverrides); e != nil {
		return d, errors.New("state_ping_override_invalid")
	}
	return d, nil
}
func atomicState(path string, d DiskState) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return errors.New("state_directory_failed")
	}
	raw, e := json.Marshal(d)
	if e != nil {
		return errors.New("state_encode_failed")
	}
	f, e := os.CreateTemp(dir, ".better-quota-*")
	if e != nil {
		return errors.New("state_create_failed")
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(raw)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil || closeErr != nil {
		return errors.New("state_fsync_failed")
	}
	if e = os.Rename(tmp, path); e != nil {
		return errors.New("state_rename_failed")
	}
	df, e := os.Open(dir)
	if e != nil {
		return errors.New("state_directory_open_failed")
	}
	defer df.Close()
	if e = df.Sync(); e != nil {
		return errors.New("state_directory_fsync_failed")
	}
	return nil
}
