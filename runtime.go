package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type Runtime struct {
	lifecycle                           sync.Mutex
	mu                                  sync.Mutex
	diskMu                              sync.Mutex
	cfg                                 Config
	gate                                *Gate
	accounts                            map[string]AccountState
	identities                          map[string]string
	nextFetch                           map[string]time.Time
	failures                            map[string]int
	busy                                map[string]bool
	resetBusy                           map[string]bool
	resetStatus                         map[string]ResetStatus
	nextReset                           map[string]time.Time
	disk                                DiskState
	diskFault                           string
	writeState                          func(string, DiskState) error
	hostCall                            func(string, []byte) ([]byte, error)
	baseURL                             string
	cancel                              context.CancelFunc
	done                                chan struct{}
	jobs                                sync.WaitGroup
	fetchSlots, resetSlots, pingSlots   chan struct{}
	lastDiscover, timeScore, nextPingAt time.Time
	nextPingByAuth                      map[string]time.Time
	discoveryError                      string
	pingCount, pingSkipped              uint64
	managementBase, managementResource  string
	configured                          bool
}

func newRuntime() *Runtime {
	c := defaultConfig()
	return &Runtime{cfg: c, gate: newGate(c), accounts: map[string]AccountState{}, identities: map[string]string{}, nextFetch: map[string]time.Time{}, failures: map[string]int{}, busy: map[string]bool{}, resetBusy: map[string]bool{}, resetStatus: map[string]ResetStatus{}, nextReset: map[string]time.Time{}, nextPingByAuth: map[string]time.Time{}, disk: emptyState(), writeState: atomicState, hostCall: nativeHostCall, baseURL: "https://chatgpt.com", fetchSlots: make(chan struct{}, 8), resetSlots: make(chan struct{}, 2), pingSlots: make(chan struct{}, 2)}
}

var runtimeState = newRuntime()

func (r *Runtime) publishLocked() {
	view := make(map[string]AccountState, len(r.accounts))
	now := time.Now()
	maxScore := 0.0
	for id, a := range r.accounts {
		a.Score = scoreQuota(id, a.Quota, r.cfg, now)
		a.NormalizedScore = 0
		a.RoutePriority = 0
		if excluded(r.cfg, id) {
			a.Score = 0
		}
		if a.Auth.enabled() && a.Error == "" && a.Score > 0 && !math.IsNaN(a.Score) && !math.IsInf(a.Score, 0) {
			maxScore = math.Max(maxScore, a.Score)
		}
		r.accounts[id] = a
	}
	for id, a := range r.accounts {
		if a.Auth.enabled() && a.Error == "" {
			a.NormalizedScore, a.RoutePriority = normalizeRoutePriority(a.Score, maxScore)
		}
		r.accounts[id] = a
		view[id] = a
	}
	r.gate.update(r.cfg, view)
}

type priorityControlPatch struct {
	Mode                   string `json:"mode"`
	AuthID                 string `json:"auth_id"`
	Enabled                bool   `json:"enabled"`
	ConfirmHardUnavailable bool   `json:"confirm_hard_unavailable"`
}

func (r *Runtime) updatePriorityControl(raw []byte) error {
	var patch priorityControlPatch
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&patch) != nil || strings.TrimSpace(patch.AuthID) == "" || (patch.Mode != "one_shot" && patch.Mode != "continuous") {
		return errors.New("invalid_priority_control_body")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return errors.New("invalid_priority_control_body")
	}
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	a, known := r.accounts[patch.AuthID]
	now := time.Now()
	hardReason := ""
	if known {
		hardReason = hardUnavailableReason(a, now)
	}
	r.mu.Unlock()
	if !known {
		return errors.New("priority_target_missing")
	}
	if patch.Enabled && hardReason != "" && !patch.ConfirmHardUnavailable {
		return errors.New("priority_confirmation_required:" + hardReason)
	}
	r.mu.Lock()
	path := r.cfg.StatePath
	r.mu.Unlock()
	r.diskMu.Lock()
	if r.diskFault != "" {
		fault := r.diskFault
		r.diskMu.Unlock()
		return errors.New(fault)
	}
	next := r.disk
	if patch.Mode == "one_shot" {
		if patch.Enabled {
			next.Settings.OneShotPriorityAuthID = patch.AuthID
		} else if next.Settings.OneShotPriorityAuthID == patch.AuthID {
			next.Settings.OneShotPriorityAuthID = ""
		}
	} else if patch.Enabled {
		next.Settings.ContinuousPriorityAuthID = patch.AuthID
	} else if next.Settings.ContinuousPriorityAuthID == patch.AuthID {
		next.Settings.ContinuousPriorityAuthID = ""
	}
	if e := r.writeState(path, next); e != nil {
		r.diskFault = "priority_state_write_failed"
		r.diskMu.Unlock()
		return errors.New("priority_state_write_failed")
	}
	r.disk = next
	r.diskMu.Unlock()
	r.mu.Lock()
	r.cfg.OneShotPriorityAuthID = next.Settings.OneShotPriorityAuthID
	r.cfg.ContinuousPriorityAuthID = next.Settings.ContinuousPriorityAuthID
	r.publishLocked()
	r.mu.Unlock()
	return nil
}

func hardUnavailableReason(a AccountState, now time.Time) string {
	if a.Auth.Disabled || a.Auth.ID == "" || !a.Auth.enabled() {
		return "disabled"
	}
	if a.Error != "" {
		return "credential_invalid"
	}
	if a.Auth.Unavailable && (a.Auth.NextRetryAfter.IsZero() || now.Before(a.Auth.NextRetryAfter)) {
		return "host_cooldown"
	}
	return ""
}

func (r *Runtime) consumeOneShotAfterAuth(id, auth string) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	claimed, ok := r.gate.priorityClaim(id)
	if !ok || claimed != auth {
		return nil
	}
	r.mu.Lock()
	path := r.cfg.StatePath
	r.mu.Unlock()
	r.diskMu.Lock()
	if r.diskFault != "" {
		fault := r.diskFault
		r.diskMu.Unlock()
		return errors.New(fault)
	}
	next := r.disk
	if next.Settings.OneShotPriorityAuthID != auth {
		r.diskMu.Unlock()
		return nil
	}
	next.Settings.OneShotPriorityAuthID = ""
	if e := r.writeState(path, next); e != nil {
		r.diskFault = "priority_state_write_failed"
		r.diskMu.Unlock()
		return errors.New("priority_state_write_failed")
	}
	r.disk = next
	r.diskMu.Unlock()
	if !r.gate.consumeOneShot(id, auth) {
		return nil
	}
	r.mu.Lock()
	r.cfg.OneShotPriorityAuthID = ""
	r.publishLocked()
	r.mu.Unlock()
	return nil
}
func (r *Runtime) stopWorker() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
		r.jobs.Wait()
		r.cancel = nil
	}
}
func (r *Runtime) stop() {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.gate.drain()
	r.stopWorker()
}
func (r *Runtime) configure(raw []byte) error {
	var req struct {
		Config []byte `json:"config_yaml"`
	}
	if json.Unmarshal(raw, &req) != nil {
		return errors.New("invalid_registration")
	}
	c, e := parseConfig(req.Config)
	if e != nil {
		return e
	}
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	changed := r.configured && r.cfg.StatePath != c.StatePath
	r.mu.Unlock()
	if changed {
		return errors.New("state_path_change_requires_drained_unload")
	}
	r.stopWorker()
	d, e := loadState(c.StatePath)
	if e != nil {
		r.gate.drain()
		return e
	}
	c, e = applyPersistedSettings(c, d)
	if e != nil {
		r.gate.drain()
		return e
	}
	r.diskMu.Lock()
	for key, old := range r.disk.Intents {
		loaded, ok := d.Intents[key]
		if !ok || loaded.RedeemID != old.RedeemID || loaded.CreditID != old.CreditID {
			r.diskMu.Unlock()
			r.gate.drain()
			return errors.New("state_intent_changed_or_missing")
		}
	}
	r.disk = d
	r.diskFault = ""
	r.diskMu.Unlock()
	r.mu.Lock()
	r.cfg = c
	r.configured = true
	r.fetchSlots = make(chan struct{}, c.FetchWorkers)
	r.nextPingByAuth = map[string]time.Time{}
	r.resetPingSchedulesLocked(c, time.Now(), nil)
	r.lastDiscover = time.Time{}
	r.publishLocked()
	r.mu.Unlock()
	r.gate.resume()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	go func() {
		defer close(done)
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		r.tick(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				r.tick(ctx)
			}
		}
	}()
	return nil
}

type settingsPatch struct {
	Strategy               *string                      `json:"strategy"`
	Floor5H                *float64                     `json:"floor_5h"`
	Floor7D                *float64                     `json:"floor_7d"`
	AutoResetEnabled       *bool                        `json:"auto_reset_enabled"`
	AutoResetAuthIDs       *[]string                    `json:"auto_reset_auth_ids"`
	AutoResetPatrolSeconds *int                         `json:"auto_reset_patrol_seconds"`
	AutoResetLeadSeconds   *int                         `json:"auto_reset_lead_seconds"`
	MaxActive              *int                         `json:"max_active"`
	ActiveOverrides        map[string]*int              `json:"active_overrides"`
	ApplyMaxActiveToAll    bool                         `json:"apply_max_active_to_all"`
	AutoPingEnabled        *bool                        `json:"auto_ping_enabled"`
	AutoPingMode           *string                      `json:"auto_ping_mode"`
	AutoPingTimes          *[]string                    `json:"auto_ping_times"`
	AutoPingOverrides      map[string]*AutoPingOverride `json:"auto_ping_overrides"`
}

func decodeSettingsPatch(raw []byte) (settingsPatch, error) {
	var patch settingsPatch
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&patch) != nil {
		return patch, errors.New("invalid_settings_body")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return patch, errors.New("invalid_settings_body")
	}
	return patch, nil
}

func (r *Runtime) updateSettings(raw []byte) error {
	patch, e := decodeSettingsPatch(raw)
	if e != nil {
		return e
	}
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()

	r.mu.Lock()
	c := r.cfg
	r.mu.Unlock()
	r.diskMu.Lock()
	if r.diskFault != "" {
		fault := r.diskFault
		r.diskMu.Unlock()
		return errors.New(fault)
	}
	next := r.disk
	settings := next.Settings
	pingChanged := false
	pingChangedIDs := map[string]bool{}
	if patch.Strategy != nil {
		if *patch.Strategy != "capacity_weighted" && *patch.Strategy != "capacity_reset" {
			r.diskMu.Unlock()
			return errors.New("invalid_strategy")
		}
		settings.Strategy = *patch.Strategy
	}
	if patch.Floor5H != nil {
		if math.IsNaN(*patch.Floor5H) || math.IsInf(*patch.Floor5H, 0) || *patch.Floor5H < 0 || *patch.Floor5H > 100 {
			r.diskMu.Unlock()
			return errors.New("invalid_floor_5h")
		}
		v := *patch.Floor5H
		settings.Floor5H = &v
	}
	if patch.Floor7D != nil {
		if math.IsNaN(*patch.Floor7D) || math.IsInf(*patch.Floor7D, 0) || *patch.Floor7D < 0 || *patch.Floor7D > 100 {
			r.diskMu.Unlock()
			return errors.New("invalid_floor_7d")
		}
		v := *patch.Floor7D
		settings.Floor7D = &v
	}
	if patch.AutoResetEnabled != nil {
		v := *patch.AutoResetEnabled
		settings.AutoResetEnabled = &v
	}
	if patch.AutoResetAuthIDs != nil {
		ids, err := normalizeIDs(*patch.AutoResetAuthIDs)
		if err != nil {
			r.diskMu.Unlock()
			return err
		}
		settings.AutoResetAuthIDs = ids
		settings.AutoResetAuthIDsSet = true
	}
	if patch.AutoResetPatrolSeconds != nil {
		if *patch.AutoResetPatrolSeconds < 30 || *patch.AutoResetPatrolSeconds > 86400 {
			r.diskMu.Unlock()
			return errors.New("invalid_reset_patrol")
		}
		settings.AutoResetPatrolSeconds = *patch.AutoResetPatrolSeconds
	}
	if patch.AutoResetLeadSeconds != nil {
		if *patch.AutoResetLeadSeconds < 60 || *patch.AutoResetLeadSeconds > 86400 {
			r.diskMu.Unlock()
			return errors.New("invalid_reset_lead")
		}
		settings.AutoResetLeadSeconds = *patch.AutoResetLeadSeconds
	}
	if patch.MaxActive != nil {
		if *patch.MaxActive < 1 || *patch.MaxActive > 10000 {
			r.diskMu.Unlock()
			return errors.New("invalid_max_active")
		}
		settings.MaxActive = *patch.MaxActive
	}
	if patch.ApplyMaxActiveToAll {
		if patch.MaxActive == nil || len(patch.ActiveOverrides) != 0 {
			r.diskMu.Unlock()
			return errors.New("invalid_all_concurrency_settings")
		}
		settings.ActiveOverrides = map[string]int{}
		settings.ActiveOverridesSet = true
	}
	if patch.ActiveOverrides != nil {
		for id, limit := range patch.ActiveOverrides {
			if strings.TrimSpace(id) == "" || (limit != nil && (*limit < 1 || *limit > 10000)) {
				r.diskMu.Unlock()
				return errors.New("invalid_active_override")
			}
		}
		// Never mutate a map shared with the live state/config before fsync.
		settings.ActiveOverrides = cloneActiveOverrides(c.ActiveOverrides)
		for id, limit := range patch.ActiveOverrides {
			if limit == nil {
				delete(settings.ActiveOverrides, id)
			} else {
				settings.ActiveOverrides[id] = *limit
			}
		}
		settings.ActiveOverridesSet = true
	}
	if patch.AutoPingEnabled != nil {
		v := *patch.AutoPingEnabled
		settings.AutoPingEnabled = &v
		pingChanged = true
	}
	if patch.AutoPingMode != nil {
		settings.AutoPingMode = *patch.AutoPingMode
		pingChanged = true
	}
	if patch.AutoPingTimes != nil {
		times, err := normalizeTimes(*patch.AutoPingTimes)
		if err != nil {
			r.diskMu.Unlock()
			return err
		}
		settings.AutoPingTimes = times
		settings.AutoPingTimesSet = true
		pingChanged = true
	}
	if patch.AutoPingOverrides != nil {
		settings.AutoPingOverrides = cloneAutoPingOverrides(c.AutoPingOverrides)
		for id, override := range patch.AutoPingOverrides {
			if strings.TrimSpace(id) == "" {
				r.diskMu.Unlock()
				return errors.New("invalid_auth_id")
			}
			if override == nil {
				delete(settings.AutoPingOverrides, id)
				pingChangedIDs[id] = true
				continue
			}
			clean, err := validateAutoPingOverride(*override)
			if err != nil {
				r.diskMu.Unlock()
				return err
			}
			settings.AutoPingOverrides[id] = clean
			pingChangedIDs[id] = true
		}
		settings.AutoPingOverridesSet = true
	}
	next.Settings = settings
	c, e = applyPersistedSettings(c, next)
	if e != nil {
		r.diskMu.Unlock()
		return e
	}
	if e = r.writeState(c.StatePath, next); e != nil {
		r.diskFault = "state_write_failed_automation_paused"
		fault := r.diskFault
		r.diskMu.Unlock()
		return errors.New(fault)
	}
	r.disk = next
	r.diskMu.Unlock()

	r.mu.Lock()
	r.cfg = c
	if pingChanged {
		// A global change must not restart a credential that has its own
		// schedule. Explicitly changed overrides are reset in the next step.
		globalIDs := map[string]bool{}
		for id := range r.accounts {
			if _, overridden := c.AutoPingOverrides[id]; !overridden {
				globalIDs[id] = true
			}
		}
		r.resetPingSchedulesLocked(c, time.Now(), globalIDs)
	}
	if len(pingChangedIDs) > 0 {
		r.resetPingSchedulesLocked(c, time.Now(), pingChangedIDs)
	}
	// A settings change should be visible to the scheduler and autoreset worker
	// immediately. A fresh patrol also re-evaluates future credit expiry safely.
	if patch.AutoResetEnabled != nil || patch.AutoResetAuthIDs != nil || patch.AutoResetPatrolSeconds != nil || patch.AutoResetLeadSeconds != nil {
		r.nextReset = map[string]time.Time{}
	}
	r.publishLocked()
	r.mu.Unlock()
	return nil
}

func (r *Runtime) transact(fn func(*DiskState)) error {
	r.mu.Lock()
	path := r.cfg.StatePath
	r.mu.Unlock()
	r.diskMu.Lock()
	defer r.diskMu.Unlock()
	if r.diskFault != "" {
		return errors.New(r.diskFault)
	}
	b, _ := json.Marshal(r.disk)
	var next DiskState
	if json.Unmarshal(b, &next) != nil {
		return errors.New("state_clone_failed")
	}
	fn(&next)
	if e := r.writeState(path, next); e != nil {
		r.diskFault = "state_write_failed_automation_paused"
		return errors.New(r.diskFault)
	}
	r.disk = next
	return nil
}
func (r *Runtime) intent(key string) (ResetIntent, bool) {
	r.diskMu.Lock()
	defer r.diskMu.Unlock()
	i, ok := r.disk.Intents[key]
	if i.Synced != nil {
		m := map[string]bool{}
		for k, v := range i.Synced {
			m[k] = v
		}
		i.Synced = m
	}
	return i, ok
}
func (r *Runtime) saveIntent(key string, i ResetIntent) error {
	return r.transact(func(d *DiskState) { d.Intents[key] = i })
}

func (r *Runtime) discover() {
	var out struct {
		Files []AuthEntry `json:"files"`
	}
	e := r.host("host.auth.list", map[string]any{}, &out)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastDiscover = time.Now()
	if e != nil {
		r.discoveryError = e.Error()
		return
	}
	r.discoveryError = ""
	live := map[string]AccountState{}
	for _, a := range out.Files {
		if a.Provider == "" {
			a.Provider = a.Type
		}
		if a.Provider != "codex" || a.ID == "" {
			continue
		}
		old := r.accounts[a.ID]
		old.Auth = a
		live[a.ID] = old
	}
	for id := range r.accounts {
		if _, ok := live[id]; !ok {
			delete(r.identities, id)
			delete(r.nextFetch, id)
			delete(r.failures, id)
		}
	}
	r.accounts = live
	r.syncPingSchedulesLocked(r.cfg, time.Now())
	r.publishLocked()
}

type pingTarget struct {
	auth AuthEntry
	at   time.Time
}

// resetPingSchedulesLocked recalculates schedules for selected credentials. A
// nil reset set means all credentials, while discovery only adds missing
// entries and removes credentials that disappeared from the host.
func (r *Runtime) resetPingSchedulesLocked(c Config, now time.Time, reset map[string]bool) {
	if r.nextPingByAuth == nil {
		r.nextPingByAuth = map[string]time.Time{}
	}
	for id := range r.accounts {
		if reset != nil && !reset[id] {
			continue
		}
		schedule := effectiveAutoPing(c, id)
		if !schedule.Enabled {
			delete(r.nextPingByAuth, id)
			continue
		}
		r.nextPingByAuth[id] = nextPingFor(schedule, now)
	}
	r.updateNextPingAtLocked()
}

func (r *Runtime) syncPingSchedulesLocked(c Config, now time.Time) {
	if r.nextPingByAuth == nil {
		r.nextPingByAuth = map[string]time.Time{}
	}
	for id := range r.nextPingByAuth {
		if _, ok := r.accounts[id]; !ok {
			delete(r.nextPingByAuth, id)
		}
	}
	for id := range r.accounts {
		if _, ok := r.nextPingByAuth[id]; ok {
			continue
		}
		schedule := effectiveAutoPing(c, id)
		if schedule.Enabled {
			r.nextPingByAuth[id] = nextPingFor(schedule, now)
		}
	}
	r.updateNextPingAtLocked()
}

func (r *Runtime) updateNextPingAtLocked() {
	r.nextPingAt = time.Time{}
	for _, at := range r.nextPingByAuth {
		if !at.IsZero() && (r.nextPingAt.IsZero() || at.Before(r.nextPingAt)) {
			r.nextPingAt = at
		}
	}
}

func (r *Runtime) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	discover := time.Since(r.lastDiscover) >= 30*time.Second
	r.mu.Unlock()
	if discover {
		r.discover()
	}
	r.mu.Lock()
	c := r.cfg
	now := time.Now()
	if now.Sub(r.timeScore) >= 30*time.Second {
		r.publishLocked()
		r.timeScore = now
	}
	for id, a := range r.accounts {
		if !a.Auth.enabled() || excluded(c, id) || r.busy[id] || now.Before(r.nextFetch[id]) {
			continue
		}
		select {
		case r.fetchSlots <- struct{}{}:
			r.busy[id] = true
			r.jobs.Add(1)
			go r.probe(ctx, a.Auth, c)
		default:
		}
	}
	groups := map[string]AuthEntry{}
	for id, key := range r.identities {
		a, ok := r.accounts[id]
		if ok && a.Auth.enabled() && !excluded(c, id) && autoResetSelected(c, id) {
			old, exists := groups[key]
			if !exists || a.Auth.ID < old.ID {
				groups[key] = a.Auth
			}
		}
	}
	for key, a := range groups {
		if !c.AutoReset.Enabled || r.resetBusy[key] || now.Before(r.nextReset[key]) {
			continue
		}
		select {
		case r.resetSlots <- struct{}{}:
			r.resetBusy[key] = true
			r.jobs.Add(1)
			go r.resetJob(ctx, key, a, c)
		default:
		}
	}
	legacyPingAt := len(r.nextPingByAuth) == 0 && !r.nextPingAt.IsZero() && !now.Before(r.nextPingAt)
	if !legacyPingAt {
		r.syncPingSchedulesLocked(c, now)
	}
	targets := []pingTarget{}
	if legacyPingAt {
		for _, a := range r.accounts {
			if a.Auth.enabled() && !excluded(c, a.Auth.ID) {
				targets = append(targets, pingTarget{auth: a.Auth, at: r.nextPingAt})
			}
		}
		r.nextPingAt = time.Time{}
	} else {
		for id, at := range r.nextPingByAuth {
			if at.IsZero() || now.Before(at) {
				continue
			}
			a, ok := r.accounts[id]
			if ok && a.Auth.enabled() && !excluded(c, id) && now.Sub(at) <= 30*time.Second {
				targets = append(targets, pingTarget{auth: a.Auth, at: at})
			} else if now.Sub(at) > 30*time.Second {
				r.pingSkipped++
			}
			schedule := effectiveAutoPing(c, id)
			if now.Sub(at) > 30*time.Second {
				r.nextPingByAuth[id] = nextPingFor(schedule, now)
			} else {
				r.nextPingByAuth[id] = nextPingAfter(schedule, at)
			}
		}
		r.updateNextPingAtLocked()
	}
	if len(targets) > 0 {
		r.jobs.Add(1)
		go r.pingBatch(ctx, targets, c)
	}
	r.mu.Unlock()
}

// Worker capacity is transient: wait within the existing 30-second trigger
// window so pools larger than two credentials are not skipped at every tick.
func (r *Runtime) pingBatch(ctx context.Context, targets []pingTarget, c Config) {
	defer r.jobs.Done()
	for _, target := range targets {
		deadline := target.at.Add(30 * time.Second)
		timer := time.NewTimer(max(0, time.Until(deadline)))
		select {
		case r.pingSlots <- struct{}{}:
			if time.Now().After(deadline) || ctx.Err() != nil {
				<-r.pingSlots
				r.mu.Lock()
				r.pingSkipped++
				r.mu.Unlock()
				timer.Stop()
				continue
			}
			r.jobs.Add(1)
			go r.pingJob(ctx, target.auth, c, target.at)
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			r.mu.Lock()
			r.pingSkipped++
			r.mu.Unlock()
			continue
		}
		timer.Stop()
	}
}
func (r *Runtime) probe(ctx context.Context, a AuthEntry, c Config) {
	defer r.jobs.Done()
	defer func() { <-r.fetchSlots; r.mu.Lock(); delete(r.busy, a.ID); r.mu.Unlock() }()
	cr, e := r.readCredentials(a, c)
	var snap Snapshot
	if e == nil {
		snap, e = r.usage(ctx, cr)
	}
	if ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.accounts[a.ID]
	if !ok || current.Auth.Index != a.Index || !current.Auth.enabled() {
		return
	}
	current.Checked = time.Now()
	current.ProbeError = ""
	if e == nil {
		current.Error = ""
		current.Quota = snap
		r.failures[a.ID] = 0
		if cr.Account != "" {
			r.identities[a.ID] = hashID(cr.Account)
		}
	} else {
		if isTransientProbeError(e) {
			current.ProbeError = e.Error()
		} else {
			current.Error = e.Error()
		}
		r.failures[a.ID]++
	}
	delay := 30 * time.Second
	if e != nil {
		if isTransientProbeError(e) {
			delay = min(30*time.Second, 5*time.Second*time.Duration(1<<min(r.failures[a.ID]-1, 3)))
		} else {
			delay *= time.Duration(1 << min(r.failures[a.ID], 4))
		}
	}
	r.nextFetch[a.ID] = time.Now().Add(delay)
	r.accounts[a.ID] = current
	r.publishLocked()
}
func (r *Runtime) resetJob(ctx context.Context, key string, a AuthEntry, c Config) {
	defer r.jobs.Done()
	defer func() {
		<-r.resetSlots
		intent, exists := r.intent(key)
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.resetBusy, key)
		now := time.Now()
		next := now.Add(time.Duration(c.AutoReset.PatrolSeconds) * time.Second)
		if exists && intent.Phase != "manual" {
			next = now.Add(10 * time.Second)
			if intent.Next.After(next) {
				next = intent.Next
			}
		}
		if at := r.nextReset[key]; at.After(now) && at.Before(next) {
			next = at
		}
		r.nextReset[key] = next
	}()
	r.stepReset(ctx, key, a, c)
}
func (r *Runtime) scheduleReset(key string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old := r.nextReset[key]; old.IsZero() || old.Before(time.Now()) || at.Before(old) {
		r.nextReset[key] = at
	}
}
func (r *Runtime) publishRecovery(key string, s Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, k := range r.identities {
		if k == key {
			a := r.accounts[id]
			a.Quota = s
			a.Error = ""
			a.ProbeError = ""
			a.Checked = time.Now()
			r.accounts[id] = a
		}
	}
	r.publishLocked()
}
func (r *Runtime) groupAccounts(key string) []AuthEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []AuthEntry{}
	for id, k := range r.identities {
		a, ok := r.accounts[id]
		// The selection chooses which credential starts the account-level patrol.
		// Once a real account is selected, every enabled alias still receives the
		// verified cooldown sync after recovery.
		if k == key && ok && a.Auth.enabled() && !excluded(r.cfg, id) {
			out = append(out, a.Auth)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Runtime) pingJob(ctx context.Context, a AuthEntry, c Config, at time.Time) {
	defer r.jobs.Done()
	defer func() { <-r.pingSlots }()
	skip := func() { r.mu.Lock(); r.pingSkipped++; r.mu.Unlock() }
	if ctx.Err() != nil {
		skip()
		return
	}
	if _, e := r.freshCredentials(a, c); e != nil {
		skip()
		return
	}
	id := "ping-" + uuid()
	if r.gate.begin(id) != nil {
		skip()
		return
	}
	defer r.gate.finish(id)
	if r.gate.admit(id, a.ID) != nil {
		skip()
		return
	}
	if !r.gate.pinInternal(id, a.ID) {
		skip()
		return
	}
	claim := at.In(shanghai).Format("2006-01-02T15:04") + "|" + hashID(a.ID)
	claimed := false
	if e := r.transact(func(d *DiskState) {
		if _, ok := d.PingClaims[claim]; ok {
			return
		}
		for k, t := range d.PingClaims {
			if at.Sub(t) > 48*time.Hour {
				delete(d.PingClaims, k)
			}
		}
		d.PingClaims[claim] = at
		claimed = true
	}); e != nil || !claimed {
		skip()
		return
	}
	if ctx.Err() != nil {
		skip()
		return
	}
	body, _ := json.Marshal(map[string]any{"model": c.AutoPing.Model, "input": "Reply OK.", "max_output_tokens": 16, "stream": false})
	var response struct {
		Status int `json:"status_code"`
	}
	e := r.host("host.model.execute", map[string]any{"entry_protocol": "openai-response", "exit_protocol": "codex", "model": c.AutoPing.Model, "stream": false, "body": body, "auth_id": a.ID, "forced_provider": "codex"}, &response)
	r.mu.Lock()
	defer r.mu.Unlock()
	if e != nil || response.Status < 200 || response.Status >= 300 {
		r.pingSkipped++
	} else {
		r.pingCount++
	}
}
