package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Reports read-only credit patrols even when no redemption is due.
type ResetStatus struct {
	CheckedAt         time.Time         `json:"checked_at"`
	Available         *int              `json:"available_count,omitempty"`
	NextExpiry        time.Time         `json:"next_expiry,omitempty"`
	Credits           []ResetCreditView `json:"credits,omitempty"`
	PreferredCreditID string            `json:"preferred_credit_id,omitempty"`
	PreferredExpiry   time.Time         `json:"preferred_expiry,omitempty"`
	Error             string            `json:"error,omitempty"`
	TransientError    string            `json:"transient_error,omitempty"`
}

type ResetCreditView struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Expires   time.Time `json:"expires_at,omitempty"`
	Preferred bool      `json:"preferred,omitempty"`
}

func preferredCredit(credits CreditList, now time.Time) (Credit, bool) {
	for _, credit := range credits.Credits {
		if credit.Status == "available" && !credit.Expires.IsZero() && credit.Expires.After(now) {
			return credit, true
		}
	}
	return Credit{}, false
}

func (r *Runtime) observeReset(key string, credits *CreditList, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.resetStatus[key]
	s.CheckedAt = time.Now().UTC()
	s.Error = ""
	s.TransientError = ""
	if credits != nil {
		n := credits.Available
		s.Available = &n
		s.NextExpiry = time.Time{}
		s.Credits = make([]ResetCreditView, 0, len(credits.Credits))
		preferred, hasPreferred := preferredCredit(*credits, s.CheckedAt)
		if hasPreferred {
			s.NextExpiry = preferred.Expires
			s.PreferredCreditID = preferred.ID
			s.PreferredExpiry = preferred.Expires
		} else {
			s.PreferredCreditID = ""
			s.PreferredExpiry = time.Time{}
		}
		for _, credit := range credits.Credits {
			s.Credits = append(s.Credits, ResetCreditView{ID: credit.ID, Status: credit.Status, Expires: credit.Expires, Preferred: hasPreferred && credit.ID == preferred.ID})
		}
	}
	if err != nil {
		if isTransientProbeError(err) {
			s.TransientError = err.Error()
		} else {
			s.Error = err.Error()
		}
	}
	r.resetStatus[key] = s
}

// One job per real account. An intent is never discarded on an uncertain result.
func (r *Runtime) stepReset(ctx context.Context, key string, a AuthEntry, c Config) {
	r.diskMu.Lock()
	fault := r.diskFault
	r.diskMu.Unlock()
	if fault != "" || ctx.Err() != nil {
		return
	}
	cr, e := r.freshCredentials(a, c)
	if e != nil || cr.Account == "" || hashID(cr.Account) != key {
		return
	}
	intent, exists := r.intent(key)
	if exists && intent.Phase == "manual" {
		return
	}
	if exists && intent.Phase == "sync" {
		r.syncCooldown(ctx, key, intent, c)
		return
	}
	if exists && time.Now().Before(intent.Next) {
		return
	}
	credits, e := r.credits(ctx, cr)
	r.observeReset(key, nil, e)
	if e != nil {
		return
	}
	r.observeReset(key, &credits, nil)
	usage, e := r.usage(ctx, cr)
	if e != nil {
		r.observeReset(key, nil, e)
		return
	}
	if exists {
		if !credits.available(intent.CreditID) && credits.Available < intent.PreCredits && recovered(intent.Before, usage) && quotaEligible(usage, c, time.Now()) == "" {
			intent.Phase = "sync"
			intent.Synced = map[string]bool{}
			intent.LastError = ""
			if r.saveIntent(key, intent) != nil {
				return
			}
			r.publishRecovery(key, usage)
			r.syncCooldown(ctx, key, intent, c)
			return
		}
		// A known successful/ambiguous credit removal is verification-only. Never pick another credit.
		if intent.Phase == "verify" || !credits.available(intent.CreditID) || intent.Attempts >= 5 {
			intent.Next = time.Now().Add(30 * time.Second)
			intent.LastError = "awaiting_recovery_confirmation"
			_ = r.saveIntent(key, intent)
			return
		}
	} else {
		var target Credit
		now := time.Now()
		for _, credit := range credits.Credits {
			if credit.Status == "available" && !credit.Expires.IsZero() && credit.Expires.After(now) && !credit.Expires.After(now.Add(time.Duration(c.AutoReset.LeadSeconds)*time.Second)) {
				target = credit
				break
			}
		}
		if target.ID == "" || credits.Available < 1 {
			for _, credit := range credits.Credits {
				if credit.Status == "available" && !credit.Expires.IsZero() {
					at := credit.Expires.Add(-time.Duration(c.AutoReset.LeadSeconds) * time.Second)
					if at.After(now) {
						r.scheduleReset(key, at)
						break
					}
				}
			}
			return
		}
		// No missing-window defaults. Do not consume a reset when no window has usage.
		used := false
		for _, w := range usage.Windows {
			used = used || w.Used > 0
		}
		if !used {
			return
		}
		intent = ResetIntent{RedeemID: uuid(), CreditID: target.ID, Phase: "pending", Before: usage, PreCredits: credits.Available, Started: now}
		if r.saveIntent(key, intent) != nil {
			return
		}
	}
	// Re-read enabled identity immediately before the irreversible upstream call.
	cr, e = r.freshCredentials(a, c)
	if e != nil || hashID(cr.Account) != key || ctx.Err() != nil {
		return
	}
	intent.Attempts++
	intent.Next = time.Now().Add(time.Duration(30*(1<<min(intent.Attempts-1, 4))) * time.Second)
	if r.saveIntent(key, intent) != nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	b, e := r.request(ctx, cr, "POST", consumePath, map[string]string{"redeem_request_id": intent.RedeemID, "credit_id": intent.CreditID})
	if e != nil {
		intent.LastError = e.Error()
		var status *HTTPError
		if errors.As(e, &status) && status.Code >= 400 && status.Code < 500 && status.Code != 429 {
			intent.Phase = "manual"
		}
		_ = r.saveIntent(key, intent)
		return
	}
	var out struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(b, &out) != nil {
		intent.LastError = "consume_result_unknown"
		_ = r.saveIntent(key, intent)
		return
	}
	switch out.Code {
	case "reset", "already_redeemed":
		intent.Phase = "verify"
		intent.Next = time.Now().Add(10 * time.Second)
	case "no_credit", "nothing_to_reset":
		intent.Phase = "manual"
		intent.LastError = out.Code
	default:
		intent.LastError = "consume_result_unknown"
	}
	_ = r.saveIntent(key, intent)
}

func (r *Runtime) syncCooldown(ctx context.Context, key string, intent ResetIntent, c Config) {
	if intent.Synced == nil {
		intent.Synced = map[string]bool{}
	}
	accounts := r.groupAccounts(key)
	if len(accounts) == 0 {
		return
	}
	for _, a := range accounts {
		if ctx.Err() != nil {
			return
		}
		if intent.Synced[a.ID] {
			continue
		}
		cr, e := r.freshCredentials(a, c)
		if e != nil || hashID(cr.Account) != key {
			return
		}
		// Recovery must still be observed before clearing any host cooldown.
		usage, e := r.usage(ctx, cr)
		if e != nil || quotaEligible(usage, c, time.Now()) != "" || !recovered(intent.Before, usage) {
			return
		}
		var out struct {
			Index string `json:"auth_index"`
		}
		if r.host("host.routing.reset_cooldown", map[string]string{"auth_index": a.Index}, &out) != nil || out.Index != a.Index {
			return
		}
		intent.Synced[a.ID] = true
		if r.saveIntent(key, intent) != nil {
			return
		}
		r.publishRecovery(key, usage)
	}
	_ = r.transact(func(d *DiskState) {
		delete(d.Intents, key)
		d.History = append(d.History, ResetRecord{key, intent.CreditID, time.Now().UTC()})
		if len(d.History) > 100 {
			d.History = d.History[len(d.History)-100:]
		}
	})
}
