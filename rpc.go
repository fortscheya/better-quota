package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

func ok(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	return json.Marshal(envelope{OK: true, Result: b})
}
func handleMethod(method string, raw []byte) ([]byte, error) {
	return runtimeState.dispatch(method, raw)
}

type HookRequest struct {
	RequestID, ToFormat string
	Headers             http.Header
	Metadata            map[string]any
}
type HookResponse struct {
	Headers         http.Header
	ClearHeaders    []string
	Terminate       bool
	StatusCode      int
	ResponseHeaders http.Header
	ResponseBody    []byte
}

func termination(code string) HookResponse {
	b, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "better_quota", "code": code, "message": "better-quota: " + code}})
	return HookResponse{ClearHeaders: []string{executionHeader}, Terminate: true, StatusCode: 429, ResponseHeaders: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, ResponseBody: b}
}
func (r *Runtime) dispatch(method string, raw []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if e := r.configure(raw); e != nil {
			return nil, e
		}
		return ok(map[string]any{"schema_version": 6, "metadata": map[string]any{"Name": "Better Quota", "Version": version, "Author": "pitr", "GitHubRepository": "https://github.com/router-for-me/CLIProxyAPI", "ConfigFields": []any{map[string]any{"Name": "strategy", "Type": "enum", "EnumValues": []string{"capacity_weighted", "capacity_reset"}, "Description": "capacity_weighted=按额度；capacity_reset=按额度和重置时间"}, map[string]any{"Name": "active_overrides", "Type": "object", "Description": "按 auth ID 覆盖并发上限"}, map[string]any{"Name": "floor_5h", "Type": "number"}, map[string]any{"Name": "floor_7d", "Type": "number"}, map[string]any{"Name": "auto_reset", "Type": "object"}, map[string]any{"Name": "auto_ping", "Type": "object"}}}, "capabilities": map[string]bool{"scheduler": true, "scheduler_across_priorities": true, "request_interceptor": true, "request_lifecycle_plugin": true, "response_stream_interceptor": true, "websocket_response_observer": true, "management_api": true, "quota_provider": true}})
	case "scheduler.pick":
		var req PickRequest
		if json.Unmarshal(raw, &req) != nil {
			return schedulerResult(reject("invalid_scheduler_request"))
		}
		return schedulerResult(r.gate.pick(context.Background(), req))
	case "request.intercept_before":
		var req HookRequest
		if json.Unmarshal(raw, &req) != nil {
			return ok(termination("invalid_hook_request"))
		}
		if e := r.gate.begin(req.RequestID); e != nil {
			return ok(termination(e.Error()))
		}
		// Replaces any untrusted client-supplied value. The scheduler also requires a live registered execution.
		return ok(HookResponse{Headers: http.Header{executionHeader: {req.RequestID}}, ClearHeaders: []string{executionHeader}})
	case "request.intercept_after":
		var req HookRequest
		if json.Unmarshal(raw, &req) != nil {
			return ok(termination("invalid_hook_request"))
		}
		auth := meta(req.Metadata, "selected_auth_id")
		r.mu.Lock()
		a, known := r.accounts[auth]
		r.mu.Unlock()
		if req.ToFormat == "codex" || (known && a.Auth.Provider == "codex") {
			if e := r.gate.admit(req.RequestID, auth); e != nil {
				return ok(termination(e.Error()))
			}
			// Consume a durable one-shot only after the selected auth has passed
			// the host's after-auth gate. A write failure keeps the target armed.
			if e := r.consumeOneShotAfterAuth(req.RequestID, auth); e != nil {
				r.gate.rollbackAdmission(req.RequestID, auth)
				return ok(termination(e.Error()))
			}
		} else {
			// A subsequent host retry switched provider; the previous upstream attempt has ended.
			r.gate.mu.Lock()
			r.gate.release(req.RequestID)
			r.gate.signal()
			r.gate.mu.Unlock()
		}
		return ok(HookResponse{ClearHeaders: []string{executionHeader}})
	case "request.complete":
		var req HookRequest
		if json.Unmarshal(raw, &req) != nil || req.RequestID == "" {
			return nil, errors.New("invalid_completion")
		}
		r.gate.finish(req.RequestID)
		return ok(struct{}{})
	case "response.intercept_stream_chunk", "websocket.response_event":
		var req HookRequest
		if json.Unmarshal(raw, &req) != nil {
			return nil, errors.New("invalid_activity")
		}
		r.gate.touch(req.RequestID)
		return ok(struct{}{})
	case "plugin.quiesce":
		r.stop()
		s := r.gate.stats()
		if s.Admitted > 0 || s.Reserved > 0 {
			return nil, errors.New("active_requests_must_drain_before_unload")
		}
		return ok(struct{}{})
	case "plugin.shutdown":
		r.stop()
		return ok(struct{}{})
	case "management.register":
		return r.registerManagement(raw)
	case "management.handle":
		return r.manage(raw)
	case "quota.identifier":
		return ok(map[string]string{"identifier": "better-quota"})
	case "quota.describe":
		return ok(map[string]any{"supported_providers": []string{"codex"}, "display_name": "Codex quota and concurrency", "supports_reset": false})
	case "quota.fetch":
		return r.cachedQuota(raw)
	default:
		return errorEnvelope("unknown_method", "unsupported method"), nil
	}
}

func schedulerResult(p PickResponse) ([]byte, error) {
	if !p.Reject {
		return ok(p)
	}
	status := 503
	switch p.RejectCode {
	case "queue_full", "queue_timeout", "credential_full":
		status = 429
	case "execution_canceled":
		status = 499
	case "invalid_scheduler_request":
		status = 400
	}
	// CPA's successful Reject response lacks HTTPStatus. A typed RPC error keeps
	// the scheduler authoritative and preserves the intended downstream status.
	detail := &envelopeError{Code: p.RejectCode, Message: p.RejectReason, HTTPStatus: status}
	if status == http.StatusTooManyRequests {
		detail.RetryAfterSeconds = 1
	}
	return json.Marshal(envelope{OK: false, Error: detail})
}

type managementRequest struct {
	Method, Path string
	Body         []byte
}
type managementResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func reply(status int, v any) ([]byte, error) {
	b, _ := json.Marshal(v)
	return ok(managementResponse{status, http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}, b})
}
func (r *Runtime) registerManagement(raw []byte) ([]byte, error) {
	var req struct{ BasePath, ResourceBasePath string }
	if json.Unmarshal(raw, &req) != nil {
		return nil, errors.New("invalid_management_registration")
	}
	suffix := strings.TrimPrefix(req.ResourceBasePath, "/v0/resource/plugins/")
	if suffix == req.ResourceBasePath || suffix == "" || strings.ContainsAny(suffix, "/\\.") || strings.TrimRight(req.BasePath, "/") != "/v0/management" {
		return nil, errors.New("invalid_management_prefix")
	}
	base := "/v0/management/plugins/" + suffix
	r.mu.Lock()
	r.managementBase = base
	r.managementResource = strings.TrimRight(req.ResourceBasePath, "/")
	r.mu.Unlock()
	return ok(map[string]any{
		"routes": []any{
			map[string]string{"Method": "GET", "Path": base + "/status"},
			map[string]string{"Method": "POST", "Path": base + "/settings"},
			map[string]string{"Method": "POST", "Path": base + "/refresh"},
			map[string]string{"Method": "POST", "Path": base + "/refresh-account"},
			map[string]string{"Method": "POST", "Path": base + "/priority-controls"},
		},
		"resources": []map[string]string{{"Path": strings.TrimRight(req.ResourceBasePath, "/") + "/dashboard", "Menu": "Better Quota", "Description": "Codex 额度、路由优先级与 autoreset"}},
	})
}

type dashboardAccount struct {
	Auth                     AuthEntry    `json:"credential"`
	Quota                    Snapshot     `json:"quota"`
	Score                    float64      `json:"-"`
	NormalizedScore          float64      `json:"-"`
	Error                    string       `json:"error,omitempty"`
	ProbeError               string       `json:"probe_error,omitempty"`
	Checked                  time.Time    `json:"checked_at"`
	RoutePriority            int          `json:"-"`
	AutoRoutePriority        int          `json:"-"`
	PriorityOverridden       bool         `json:"-"`
	HighestPriority          bool         `json:"-"`
	QuantizedScore           int          `json:"quantized_score"`
	OneShotSelected          bool         `json:"one_shot_selected"`
	ContinuousSelected       bool         `json:"continuous_selected"`
	HardUnavailable          bool         `json:"hard_unavailable"`
	HardUnavailableReason    string       `json:"hard_unavailable_reason,omitempty"`
	AutoResetSelected        bool         `json:"auto_reset_selected"`
	ResetAccountKey          string       `json:"reset_account_key,omitempty"`
	Reset                    *ResetStatus `json:"reset,omitempty"`
	Active                   int          `json:"active"`
	ConcurrencyLimit         int          `json:"concurrency_limit"`
	ConcurrencyUsed          int          `json:"concurrency_used"`
	ConcurrencyActive        int          `json:"concurrency_active"`
	ConcurrencyReserved      int          `json:"concurrency_reserved"`
	ConcurrencyAvailable     int          `json:"concurrency_available"`
	ConcurrencyBlocked       int          `json:"concurrency_blocked"`
	ConcurrencyCooling       bool         `json:"concurrency_cooling"`
	ConcurrencyBlockedReason string       `json:"concurrency_blocked_reason,omitempty"`
	ConcurrencyOverridden    bool         `json:"concurrency_overridden"`
	FailureCount             int          `json:"failure_count,omitempty"`
	NextRetryAt              time.Time    `json:"next_retry_at,omitempty"`
	ProbeRetryAt             time.Time    `json:"probe_retry_at,omitempty"`
}

func copyResetStatus(in ResetStatus) *ResetStatus {
	out := in
	out.Credits = append([]ResetCreditView{}, in.Credits...)
	if in.Available != nil {
		n := *in.Available
		out.Available = &n
	}
	return &out
}

func (r *Runtime) dashboardAccountsLocked(c Config, gateAccounts map[string]CredentialGateStats) []dashboardAccount {
	rows := make([]dashboardAccount, 0, len(r.accounts))
	now := time.Now()
	for id, a := range r.accounts {
		priority := a.RoutePriority
		gate := gateAccounts[id]
		limit := gate.Limit
		if limit < 1 {
			limit = c.MaxActive
			if override := c.ActiveOverrides[id]; override > 0 {
				limit = override
			}
		}
		used := gate.Active + gate.Reserved
		cooling := gate.BlockedReason == "host_cooldown"
		blocked := 0
		if gate.BlockedReason != "" && used < limit {
			blocked = limit - used
		}
		row := dashboardAccount{
			Auth:                     a.Auth,
			Quota:                    a.Quota,
			Score:                    a.Score,
			NormalizedScore:          a.NormalizedScore,
			Error:                    a.Error,
			ProbeError:               a.ProbeError,
			Checked:                  a.Checked,
			RoutePriority:            priority,
			AutoRoutePriority:        priority,
			PriorityOverridden:       false,
			AutoResetSelected:        c.AutoReset.Enabled && autoResetSelected(c, id),
			QuantizedScore:           priority,
			OneShotSelected:          c.OneShotPriorityAuthID == id,
			ContinuousSelected:       c.ContinuousPriorityAuthID == id,
			HardUnavailableReason:    hardUnavailableReason(a, now),
			ResetAccountKey:          r.identities[id],
			Active:                   used,
			ConcurrencyLimit:         limit,
			ConcurrencyUsed:          used,
			ConcurrencyActive:        gate.Active,
			ConcurrencyReserved:      gate.Reserved,
			ConcurrencyAvailable:     max(0, limit-used-blocked),
			ConcurrencyBlocked:       blocked,
			ConcurrencyCooling:       cooling,
			ConcurrencyBlockedReason: gate.BlockedReason,
			ConcurrencyOverridden:    c.ActiveOverrides[id] > 0,
			FailureCount:             r.failures[id],
			NextRetryAt:              a.Auth.NextRetryAfter,
		}
		row.HardUnavailable = row.HardUnavailableReason != ""
		if (a.Error != "" || a.ProbeError != "") && r.nextFetch[id].After(now) {
			row.ProbeRetryAt = r.nextFetch[id]
		}
		if status, ok := r.resetStatus[r.identities[id]]; ok {
			row.Reset = copyResetStatus(status)
		}
		rows = append(rows, row)
	}
	// Keep the legacy in-memory indicator populated for callers that still use
	// the Go snapshot. It is intentionally omitted from the management JSON.
	bestPriority := 0
	hasBest := false
	available := func(row dashboardAccount) bool {
		return row.Auth.enabled() && !excluded(c, row.Auth.ID) && row.Error == "" &&
			quotaEligible(row.Quota, c, now) == "" && row.Score > 0 && row.ConcurrencyAvailable > 0
	}
	for _, row := range rows {
		if available(row) && (!hasBest || row.QuantizedScore > bestPriority) {
			hasBest = true
			bestPriority = row.QuantizedScore
		}
	}
	if hasBest {
		for i := range rows {
			rows[i].HighestPriority = available(rows[i]) && rows[i].QuantizedScore == bestPriority
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Auth.ID < rows[j].Auth.ID })
	return rows
}

func priorityStatus(c Config, accounts map[string]AccountState, now time.Time) map[string]any {
	target := c.OneShotPriorityAuthID
	mode := "one_shot"
	if target == "" {
		target = c.ContinuousPriorityAuthID
		mode = "continuous"
	}
	status := map[string]any{
		"one_shot_auth_id":   c.OneShotPriorityAuthID,
		"continuous_auth_id": c.ContinuousPriorityAuthID,
		"target_auth_id":     target,
		"target_mode":        "",
		"target_unavailable": false,
		"target_reason":      "",
	}
	if target == "" {
		return status
	}
	status["target_mode"] = mode
	a, ok := accounts[target]
	if !ok {
		status["target_unavailable"] = true
		status["target_reason"] = "priority_target_missing"
		return status
	}
	reason := hardUnavailableReason(a, now)
	if reason == "" {
		reason = priorityQuotaEligible(a.Quota, c, now)
	}
	if reason != "" {
		status["target_unavailable"] = true
		status["target_reason"] = reason
	}
	return status
}
func (r *Runtime) manage(raw []byte) ([]byte, error) {
	var req managementRequest
	if json.Unmarshal(raw, &req) != nil {
		return reply(400, map[string]string{"error": "invalid_request"})
	}
	r.mu.Lock()
	base := r.managementBase
	resource := r.managementResource
	r.mu.Unlock()
	if base == "" {
		return reply(404, map[string]string{"error": "not_found"})
	}
	if req.Path == resource+"/dashboard" && req.Method == "GET" {
		page := strings.ReplaceAll(dashboardHTML, "__API_BASE__", base)
		page = strings.ReplaceAll(page, "__EMBEDDED_CLIENT__", embeddedClientJS)
		page = strings.ReplaceAll(page, "__CONCURRENCY_CLIENT__", concurrencyClientJS)
		return ok(managementResponse{200, http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"}, "Content-Security-Policy": {"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'self'"}, "Referrer-Policy": {"no-referrer"}}, []byte(page)})
	}
	if req.Path == base+"/status" && req.Method == "GET" {
		r.mu.Lock()
		gate := r.gate.stats()
		accounts := []AccountState{}
		for _, a := range r.accounts {
			accounts = append(accounts, a)
		}
		sort.Slice(accounts, func(i, j int) bool { return accounts[i].Auth.ID < accounts[j].Auth.ID })
		cfg := r.cfg
		dashboard := r.dashboardAccountsLocked(cfg, gate.Credentials)
		priority := priorityStatus(cfg, r.accounts, time.Now())
		resetStatus := make(map[string]ResetStatus, len(r.resetStatus))
		for key, state := range r.resetStatus {
			resetStatus[key] = state
		}
		last, discoverErr, ping, pingSkip, next := r.lastDiscover, r.discoveryError, r.pingCount, r.pingSkipped, r.nextPingAt
		r.mu.Unlock()
		r.diskMu.Lock()
		intents := map[string]ResetIntent{}
		for k, i := range r.disk.Intents {
			intents[k] = i
		}
		history := append([]ResetRecord{}, r.disk.History...)
		fault := r.diskFault
		r.diskMu.Unlock()
		return reply(200, map[string]any{"version": version, "config": cfg, "auto_ping_overrides": cfg.AutoPingOverrides, "accounts": accounts, "dashboard_accounts": dashboard, "gate": gate, "priority": priority, "one_shot_priority_auth_id": cfg.OneShotPriorityAuthID, "continuous_priority_auth_id": cfg.ContinuousPriorityAuthID, "priority_target_unavailable": priority["target_unavailable"], "priority_target_reason": priority["target_reason"], "reset_accounts": resetStatus, "reset_intents": intents, "reset_history": history, "automation_error": fault, "last_discovery": last, "discovery_error": discoverErr, "ping_success": ping, "ping_skipped": pingSkip, "next_ping_at": next, "score_period_seconds": 30, "strategy_options": []map[string]string{{"id": "capacity_weighted", "name": "按额度", "description": "按剩余额度比例分配流量。"}, {"id": "capacity_reset", "name": "按额度和重置时间", "description": "同时考虑剩余额度与各窗口距离重置的时间。"}}, "scope": "single CPA process; Codex OAuth quotas; no physical weight writes"})
	}
	if req.Path == base+"/priorities" && req.Method == "POST" {
		return reply(410, map[string]string{"error": "route_priority_removed"})
	}
	if req.Path == base+"/priority-controls" && req.Method == "POST" {
		if e := r.updatePriorityControl(req.Body); e != nil {
			status := 400
			if strings.HasPrefix(e.Error(), "priority_confirmation_required:") {
				status = 409
			}
			return reply(status, map[string]any{"error": e.Error(), "requires_confirmation": status == 409})
		}
		return reply(200, map[string]string{"status": "saved"})
	}
	if req.Path == base+"/settings" && req.Method == "POST" {
		if e := r.updateSettings(req.Body); e != nil {
			status := 500
			if strings.HasPrefix(e.Error(), "invalid_") || strings.HasPrefix(e.Error(), "state_") {
				status = 400
			}
			return reply(status, map[string]string{"error": e.Error()})
		}
		return reply(200, map[string]string{"status": "saved"})
	}
	if req.Path == base+"/refresh" && req.Method == "POST" {
		r.mu.Lock()
		r.lastDiscover = time.Time{}
		r.mu.Unlock()
		return reply(202, map[string]string{"status": "discovery_queued_probe_backoff_preserved"})
	}
	if req.Path == base+"/refresh-account" && req.Method == "POST" {
		var body struct {
			AuthID string `json:"auth_id"`
		}
		if json.Unmarshal(req.Body, &body) != nil || strings.TrimSpace(body.AuthID) == "" {
			return reply(400, map[string]string{"error": "invalid_auth_id"})
		}
		r.mu.Lock()
		a, ok := r.accounts[body.AuthID]
		if !ok {
			r.mu.Unlock()
			return reply(404, map[string]string{"error": "auth_not_found"})
		}
		if !a.Auth.enabled() {
			r.mu.Unlock()
			return reply(400, map[string]string{"error": "credential_disabled"})
		}
		if r.busy[body.AuthID] {
			r.mu.Unlock()
			return reply(409, map[string]string{"error": "probe_in_progress"})
		}
		select {
		case r.fetchSlots <- struct{}{}:
		default:
			r.mu.Unlock()
			return reply(409, map[string]string{"error": "probe_busy"})
		}
		r.busy[body.AuthID] = true
		c := r.cfg
		r.jobs.Add(1)
		r.mu.Unlock()
		go r.probe(context.Background(), a.Auth, c)
		return reply(202, map[string]string{"status": "account_probe_queued"})
	}
	return reply(404, map[string]string{"error": "not_found"})
}

func (r *Runtime) cachedQuota(raw []byte) ([]byte, error) {
	var req struct {
		Index    string `json:"auth_index"`
		Provider string `json:"provider"`
	}
	if json.Unmarshal(raw, &req) != nil || req.Provider != "codex" {
		return nil, errors.New("unsupported_quota_request")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.accounts {
		if a.Auth.Index == req.Index && a.Auth.enabled() {
			s := a.Quota
			if s.Fetched.IsZero() || time.Since(s.Fetched) > time.Duration(r.cfg.MaxStaleSeconds)*time.Second {
				return nil, errors.New("quota_stale")
			}
			buckets := []any{}
			for _, w := range s.Windows {
				if w.Reset <= time.Now().Unix() {
					return nil, errors.New("quota_reset_pending")
				}
				buckets = append(buckets, map[string]any{"window": time.Duration(w.Seconds * int64(time.Second)).String(), "remainingFraction": 1 - w.Used/100, "resetTime": time.Unix(w.Reset, 0).UTC().Format(time.RFC3339)})
			}
			return ok(map[string]any{"subscription": map[string]string{"plan": s.Plan}, "groups": []any{map[string]any{"displayName": "Codex", "buckets": buckets}}})
		}
	}
	return nil, errors.New("quota_unknown")
}
