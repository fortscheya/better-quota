package main

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type Candidate struct {
	ID, Provider, Status string
	Priority             int
	Attributes           map[string]string
	Metadata             map[string]any
}
type PickRequest struct {
	Provider   string
	Providers  []string
	Model      string
	Stream     bool
	Candidates []Candidate
	Options    struct {
		Headers  map[string][]string
		Metadata map[string]any
	}
}
type PickResponse struct {
	AuthID                   string
	Handled, Reject          bool
	RejectCode, RejectReason string
}
type AccountState struct {
	Auth            AuthEntry `json:"credential"`
	Quota           Snapshot  `json:"quota"`
	Score           float64   `json:"-"`
	NormalizedScore float64   `json:"-"`
	RoutePriority   int       `json:"-"`
	Error           string    `json:"error,omitempty"`
	ProbeError      string    `json:"probe_error,omitempty"`
	Checked         time.Time `json:"checked_at"`
}

type lease struct {
	Auth              string
	Admitted          bool
	Priority          bool
	Created, Activity time.Time
}
type execution struct {
	At           time.Time
	Done         chan struct{}
	PriorityAuth string
}
type binding struct {
	Auth string
	At   time.Time
}
type Gate struct {
	mu            sync.Mutex
	cfg           Config
	accounts      map[string]AccountState
	leases        map[string]lease
	executions    map[string]execution
	finished      map[string]time.Time
	active        map[string]int
	waiting       map[string]bool
	internalPins  map[string]string
	sticky        map[string]binding
	smooth        map[string]map[string]float64
	oneShotClaims map[string]string
	changed       chan struct{}
	draining      bool
	rejected      uint64
	now           func() time.Time
}

func newGate(c Config) *Gate {
	return &Gate{cfg: c, accounts: map[string]AccountState{}, leases: map[string]lease{}, executions: map[string]execution{}, finished: map[string]time.Time{}, active: map[string]int{}, waiting: map[string]bool{}, internalPins: map[string]string{}, sticky: map[string]binding{}, smooth: map[string]map[string]float64{}, oneShotClaims: map[string]string{}, changed: make(chan struct{}), now: time.Now}
}
func (g *Gate) signal() { close(g.changed); g.changed = make(chan struct{}) }
func (g *Gate) update(c Config, accounts map[string]AccountState) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cfg = c
	g.accounts = accounts
	g.signal()
}
func (g *Gate) priorityClaim(id string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	auth, ok := g.oneShotClaims[id]
	return auth, ok
}
func (g *Gate) consumeOneShot(id, auth string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.oneShotClaims[id] != auth || g.cfg.OneShotPriorityAuthID != auth {
		return false
	}
	g.cfg.OneShotPriorityAuthID = ""
	delete(g.oneShotClaims, id)
	g.signal()
	return true
}
func (g *Gate) rollbackAdmission(id, auth string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if l, ok := g.leases[id]; ok && l.Auth == auth && l.Admitted {
		l.Admitted = false
		l.Activity = g.now()
		g.leases[id] = l
		g.signal()
	}
}
func (g *Gate) begin(id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweep(g.now())
	if id == "" {
		return errors.New("missing_execution")
	}
	if _, ok := g.finished[id]; ok {
		return errors.New("execution_already_finished")
	}
	if _, ok := g.executions[id]; !ok {
		g.executions[id] = execution{At: g.now(), Done: make(chan struct{})}
	}
	return nil
}
func (g *Gate) release(id string) {
	if l, ok := g.leases[id]; ok {
		g.active[l.Auth]--
		if g.active[l.Auth] == 0 {
			delete(g.active, l.Auth)
		}
		delete(g.leases, id)
	}
}
func (g *Gate) finish(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.release(id)
	for auth, pin := range g.internalPins {
		if pin == id {
			delete(g.internalPins, auth)
		}
	}
	if e, ok := g.executions[id]; ok {
		close(e.Done)
		delete(g.executions, id)
	}
	delete(g.oneShotClaims, id)
	g.finished[id] = g.now()
	g.signal()
}
func (g *Gate) pinInternal(id, auth string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	l, ok := g.leases[id]
	if !ok || !l.Admitted || l.Auth != auth || g.internalPins[auth] != "" {
		return false
	}
	g.internalPins[auth] = id
	return true
}
func (g *Gate) touch(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if l, ok := g.leases[id]; ok {
		l.Activity = g.now()
		g.leases[id] = l
	}
}
func (g *Gate) drain()  { g.mu.Lock(); defer g.mu.Unlock(); g.draining = true; g.signal() }
func (g *Gate) resume() { g.mu.Lock(); defer g.mu.Unlock(); g.draining = false; g.signal() }
func (g *Gate) sweep(now time.Time) {
	for id, l := range g.leases {
		if !l.Admitted && now.Sub(l.Created) >= time.Duration(g.cfg.ReservationSeconds)*time.Second {
			g.release(id)
		}
	}
	for id, at := range g.finished {
		if now.Sub(at) > 10*time.Minute {
			delete(g.finished, id)
		}
	}
	for id, e := range g.executions {
		if _, ok := g.leases[id]; !ok && !g.waiting[id] && now.Sub(e.At) > 10*time.Minute {
			close(e.Done)
			delete(g.executions, id)
			delete(g.oneShotClaims, id)
		}
	}
	for k, b := range g.sticky {
		if now.Sub(b.At) > time.Duration(g.cfg.StickySeconds)*time.Second {
			delete(g.sticky, k)
		}
	}
}
func (g *Gate) limit(id string) int {
	if n := g.cfg.ActiveOverrides[id]; n > 0 {
		return n
	}
	return g.cfg.MaxActive
}
func (g *Gate) eligible(id string, now time.Time) bool {
	a, ok := g.accounts[id]
	if !ok || !a.Auth.enabled() || a.Error != "" {
		return false
	}
	if a.Auth.Unavailable && (a.Auth.NextRetryAfter.IsZero() || now.Before(a.Auth.NextRetryAfter)) {
		return false
	}
	return quotaEligible(a.Quota, g.cfg, now) == "" && a.Score > 0
}
func (g *Gate) priorityUnavailableReason(id string, now time.Time) string {
	a, ok := g.accounts[id]
	if !ok {
		return "priority_target_missing"
	}
	if !a.Auth.enabled() {
		return "priority_target_unavailable:disabled"
	}
	if a.Error != "" {
		return "priority_target_unavailable:credential_invalid"
	}
	if a.Auth.Unavailable && (a.Auth.NextRetryAfter.IsZero() || now.Before(a.Auth.NextRetryAfter)) {
		return "priority_target_unavailable:host_cooldown"
	}
	if reason := priorityQuotaEligible(a.Quota, g.cfg, now); reason != "" {
		return "priority_target_unavailable:" + reason
	}
	return ""
}
func (g *Gate) priorityEligible(id string, now time.Time) bool {
	return g.priorityUnavailableReason(id, now) == ""
}
func (g *Gate) free(id, request string) bool {
	n := g.active[id]
	if l, ok := g.leases[request]; ok && l.Auth == id {
		n--
	}
	return n < g.limit(id)
}
func meta(m map[string]any, key string) string { s, _ := m[key].(string); return s }
func header(h map[string][]string, key string) string {
	for k, v := range h {
		if strings.EqualFold(k, key) && len(v) == 1 {
			return v[0]
		}
	}
	return ""
}
func scopeKey(req PickRequest, session string) string {
	return hashID(meta(req.Options.Metadata, "caller_scope")) + "|" + req.Model + "|" + hashID(session)
}
func (g *Gate) bind(req PickRequest, id string, now time.Time) {
	s := meta(req.Options.Metadata, "canonical_session_id")
	if s == "" {
		s = meta(req.Options.Metadata, "execution_session_id")
	}
	if s != "" {
		if len(g.sticky) >= 10000 {
			var oldest string
			var at time.Time
			for k, b := range g.sticky {
				if oldest == "" || b.At.Before(at) {
					oldest = k
					at = b.At
				}
			}
			delete(g.sticky, oldest)
		}
		g.sticky[scopeKey(req, s)] = binding{id, now}
	}
}
func reject(code string) PickResponse {
	return PickResponse{Handled: true, Reject: true, RejectCode: code, RejectReason: "better-quota: " + code}
}

type chooseResult struct {
	auth     string
	eligible bool
	priority bool
	waitCode string
}

func (g *Gate) priorityTarget(request string) (string, bool) {
	if auth := g.oneShotClaims[request]; auth != "" {
		return auth, true
	}
	if auth := g.executions[request].PriorityAuth; auth != "" {
		return auth, false
	}
	if g.cfg.OneShotPriorityAuthID != "" {
		return g.cfg.OneShotPriorityAuthID, true
	}
	if g.cfg.ContinuousPriorityAuthID != "" {
		return g.cfg.ContinuousPriorityAuthID, false
	}
	return "", false
}

func (g *Gate) candidateByID(req PickRequest, id string) bool {
	for _, c := range req.Candidates {
		if c.ID == id && c.Provider == "codex" {
			return true
		}
	}
	return false
}

func (g *Gate) choose(req PickRequest, request string, now time.Time) chooseResult {
	// A nested auto-ping already owns its exact-auth slot. It must not claim
	// the next external request's priority target or switch the pinned auth.
	if pin := meta(req.Options.Metadata, "pinned_auth_id"); pin != "" && meta(req.Options.Metadata, "source") == "plugin_host_model_callback" && g.internalPins[pin] == request {
		if l, ok := g.leases[request]; ok && l.Auth == pin && l.Admitted && g.eligible(pin, now) && g.candidateByID(req, pin) {
			return chooseResult{auth: pin, eligible: true}
		}
		return chooseResult{}
	}
	// A configured priority target is evaluated before an ordinary execution
	// lease. This lets a newly armed target take effect at the next scheduler
	// callback while the forced lease itself remains stable across retries.
	if target, oneShot := g.priorityTarget(request); target != "" {
		if oneShot {
			for id := range g.oneShotClaims {
				if id != request && g.oneShotClaims[id] == target {
					return chooseResult{eligible: true, waitCode: "priority_target_claimed"}
				}
			}
		}
		if !g.candidateByID(req, target) {
			return chooseResult{eligible: true, waitCode: "priority_target_missing"}
		}
		if reason := g.priorityUnavailableReason(target, now); reason != "" {
			return chooseResult{eligible: true, waitCode: reason}
		}
		if oneShot && g.oneShotClaims[request] == "" {
			g.oneShotClaims[request] = target
		}
		return chooseResult{auth: target, eligible: true, priority: true}
	}

	// With no priority target, a request that already owns a lease keeps it
	// across retries. This is the ordinary sticky execution guarantee.
	if l, ok := g.leases[request]; ok && g.candidateByID(req, l.Auth) {
		if l.Priority {
			if g.priorityEligible(l.Auth, now) {
				return chooseResult{auth: l.Auth, eligible: true, priority: true}
			}
			return chooseResult{eligible: true, waitCode: g.priorityUnavailableReason(l.Auth, now)}
		}
		if g.eligible(l.Auth, now) && g.free(l.Auth, request) {
			return chooseResult{auth: l.Auth, eligible: true}
		}
	}

	choices := make([]Candidate, 0, len(req.Candidates))
	seen := map[string]bool{}
	eligible := false
	for _, c := range req.Candidates {
		if c.Provider != "codex" || seen[c.ID] {
			continue
		}
		seen[c.ID] = true
		if !g.eligible(c.ID, now) {
			continue
		}
		eligible = true
		if g.free(c.ID, request) {
			choices = append(choices, c)
		}
	}
	if len(choices) == 0 {
		return chooseResult{eligible: eligible}
	}

	// Sticky and parent bindings outrank the dynamic score tier. They still
	// need a fresh quota snapshot and a normal free slot.
	byID := map[string]bool{}
	for _, c := range choices {
		byID[c.ID] = true
	}
	for _, s := range []string{meta(req.Options.Metadata, "canonical_session_id"), meta(req.Options.Metadata, "execution_session_id"), meta(req.Options.Metadata, "parent_session_id")} {
		if s == "" {
			continue
		}
		if b, ok := g.sticky[scopeKey(req, s)]; ok && now.Sub(b.At) <= time.Duration(g.cfg.StickySeconds)*time.Second && byID[b.Auth] {
			return chooseResult{auth: b.Auth, eligible: true}
		}
	}

	// RoutePriority is the 0..10000 quantized score. It ranks the candidate
	// set only; utilization decides among the first three ranks.
	sort.Slice(choices, func(i, j int) bool {
		left, right := g.accounts[choices[i].ID], g.accounts[choices[j].ID]
		if left.RoutePriority != right.RoutePriority {
			return left.RoutePriority > right.RoutePriority
		}
		return choices[i].ID < choices[j].ID
	})
	if len(choices) > 3 {
		choices = choices[:3]
	}
	chosen := choices[0].ID
	utilization := func(id string) float64 {
		limit := g.limit(id)
		if limit < 1 {
			return math.Inf(1)
		}
		return float64(g.active[id]) / float64(limit)
	}
	for _, c := range choices[1:] {
		left, right := utilization(c.ID), utilization(chosen)
		if left < right || (left == right && c.ID < chosen) {
			chosen = c.ID
		}
	}
	return chooseResult{auth: chosen, eligible: true}
}

func (g *Gate) pick(ctx context.Context, req PickRequest) PickResponse {
	hasCodex := req.Provider == "codex"
	mixed := false
	for _, c := range req.Candidates {
		hasCodex = hasCodex || c.Provider == "codex"
		mixed = mixed || (c.Provider != "" && c.Provider != "codex")
	}
	if !hasCodex || mixed {
		return PickResponse{}
	}
	id := header(req.Options.Headers, executionHeader)
	g.mu.Lock()
	defer g.mu.Unlock()
	// Host model callbacks skip the calling plugin's own request hooks. Only our
	// already admitted, exact-auth nested call can reuse its explicit local slot.
	if id == "" && meta(req.Options.Metadata, "source") == "plugin_host_model_callback" {
		pin := meta(req.Options.Metadata, "pinned_auth_id")
		id = g.internalPins[pin]
	}
	if _, ok := g.executions[id]; !ok || id == "" {
		g.rejected++
		return reject("missing_execution_context")
	}
	if g.waiting[id] {
		g.rejected++
		return reject("duplicate_waiter")
	}
	done := g.executions[id].Done
	deadline := time.NewTimer(time.Duration(g.cfg.WaitSeconds) * time.Second)
	defer deadline.Stop()
	queued := false
	defer func() { delete(g.waiting, id) }()
	for {
		now := g.now()
		g.sweep(now)
		if g.draining {
			g.rejected++
			return reject("draining")
		}
		if _, ok := g.executions[id]; !ok {
			g.rejected++
			return reject("execution_canceled")
		}
		choice := g.choose(req, id, now)
		if choice.auth != "" {
			if choice.priority {
				e := g.executions[id]
				e.PriorityAuth = choice.auth
				g.executions[id] = e
			}
			if old, ok := g.leases[id]; !ok || old.Auth != choice.auth {
				g.release(id)
				g.leases[id] = lease{Auth: choice.auth, Priority: choice.priority, Created: now, Activity: now}
				g.active[choice.auth]++
			} else if choice.priority && !old.Priority {
				old.Priority = true
				g.leases[id] = old
			}
			if !choice.priority {
				g.bind(req, choice.auth, now)
			}
			return PickResponse{AuthID: choice.auth, Handled: true}
		}
		if choice.waitCode != "" && choice.waitCode != "priority_target_claimed" {
			g.rejected++
			code := choice.waitCode
			if strings.HasPrefix(code, "priority_target_unavailable:") {
				code = "priority_target_unavailable"
			}
			return reject(code)
		}
		if !choice.eligible {
			g.rejected++
			return reject("quota_unavailable")
		}
		if !queued {
			if g.cfg.WaitSeconds == 0 || len(g.waiting) >= g.cfg.QueueLimit {
				g.rejected++
				return reject("queue_full")
			}
			g.waiting[id] = true
			queued = true
		}
		changed := g.changed
		g.mu.Unlock()
		tick := time.NewTimer(100 * time.Millisecond)
		reason := ""
		select {
		case <-changed:
		case <-tick.C:
		case <-deadline.C:
			reason = "queue_timeout"
		case <-done:
			reason = "execution_canceled"
		case <-ctx.Done():
			reason = "execution_canceled"
		}
		tick.Stop()
		g.mu.Lock()
		if reason != "" {
			g.rejected++
			return reject(reason)
		}
	}
}

func (g *Gate) admit(id, auth string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	g.sweep(now)
	if g.draining {
		return errors.New("draining")
	}
	if _, ok := g.executions[id]; !ok {
		return errors.New("missing_execution_context")
	}
	forcedAuth := g.executions[id].PriorityAuth
	if forcedAuth != "" && forcedAuth != auth {
		return errors.New("priority_target_unavailable")
	}
	if l, ok := g.leases[id]; ok && l.Auth == auth && l.Admitted {
		if l.Priority {
			if reason := g.priorityUnavailableReason(auth, now); reason != "" {
				if strings.HasPrefix(reason, "priority_target_unavailable:") {
					return errors.New("priority_target_unavailable")
				}
				return errors.New(reason)
			}
		}
		l.Activity = now
		g.leases[id] = l
		return nil
	}
	if l, ok := g.leases[id]; ok && l.Priority && l.Auth != auth {
		return errors.New("priority_target_unavailable")
	}
	priority := forcedAuth != ""
	if l, ok := g.leases[id]; ok && l.Auth == auth {
		priority = l.Priority
	}
	if priority {
		if reason := g.priorityUnavailableReason(auth, now); reason != "" {
			if strings.HasPrefix(reason, "priority_target_unavailable:") {
				return errors.New("priority_target_unavailable")
			}
			return errors.New(reason)
		}
	} else {
		if !g.eligible(auth, now) {
			return errors.New("quota_unavailable")
		}
		if !g.free(auth, id) {
			return errors.New("credential_full")
		}
	}
	if l, ok := g.leases[id]; !ok || l.Auth != auth {
		g.release(id)
		g.active[auth]++
	}
	g.leases[id] = lease{Auth: auth, Admitted: true, Priority: priority, Created: now, Activity: now}
	g.signal()
	return nil
}

type GateStats struct {
	Active         map[string]int                 `json:"active"`
	Credentials    map[string]CredentialGateStats `json:"credentials"`
	Reserved       int                            `json:"reserved"`
	Admitted       int                            `json:"admitted"`
	Queued         int                            `json:"queued"`
	Rejected       uint64                         `json:"rejected"`
	Draining       bool                           `json:"draining"`
	OldestActivity time.Time                      `json:"oldest_activity,omitempty"`
}

type CredentialGateStats struct {
	Limit         int    `json:"limit"`
	Active        int    `json:"active"`
	Reserved      int    `json:"reserved"`
	BlockedReason string `json:"blocked_reason,omitempty"`
}

// Read-only availability for the dashboard. Host cooldown is a credential
// snapshot; actual model-specific candidate eligibility remains owned by CPA.
func (g *Gate) blockedReason(id string, now time.Time) string {
	a, ok := g.accounts[id]
	if !ok {
		return "unknown"
	}
	if g.draining {
		return "draining"
	}
	if !a.Auth.enabled() {
		return "disabled"
	}
	if excluded(g.cfg, id) {
		return "excluded"
	}
	if a.Auth.Unavailable && (a.Auth.NextRetryAfter.IsZero() || now.Before(a.Auth.NextRetryAfter)) {
		return "host_cooldown"
	}
	if a.Error == "unsupported" {
		return "unsupported"
	}
	if a.Error != "" {
		return "probe_error"
	}
	if reason := quotaEligible(a.Quota, g.cfg, now); reason != "" {
		return reason
	}
	if a.Score <= 0 {
		return "quota_unavailable"
	}
	return ""
}

func (g *Gate) stats() GateStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	g.sweep(now)
	s := GateStats{Active: map[string]int{}, Credentials: map[string]CredentialGateStats{}, Queued: len(g.waiting), Rejected: g.rejected, Draining: g.draining}
	for id := range g.accounts {
		s.Credentials[id] = CredentialGateStats{Limit: g.limit(id), BlockedReason: g.blockedReason(id, now)}
	}
	for k, v := range g.active {
		s.Active[k] = v
	}
	for _, l := range g.leases {
		account := s.Credentials[l.Auth]
		if account.Limit == 0 {
			account.Limit = g.limit(l.Auth)
		}
		if l.Admitted {
			s.Admitted++
			account.Active++
		} else {
			s.Reserved++
			account.Reserved++
		}
		s.Credentials[l.Auth] = account
		if s.OldestActivity.IsZero() || l.Activity.Before(s.OldestActivity) {
			s.OldestActivity = l.Activity
		}
	}
	return s
}
