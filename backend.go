package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type AuthEntry struct {
	ID             string    `json:"id"`
	Index          string    `json:"auth_index"`
	Email          string    `json:"email,omitempty"`
	Provider       string    `json:"provider"`
	Type           string    `json:"type"`
	Disabled       bool      `json:"disabled"`
	RuntimeOnly    bool      `json:"runtime_only"`
	Priority       int       `json:"priority"`
	Unavailable    bool      `json:"unavailable,omitempty"`
	NextRetryAfter time.Time `json:"next_retry_after,omitempty"`
}

func (a AuthEntry) enabled() bool {
	return a.ID != "" && a.Index != "" && !a.Disabled && (a.Provider == "codex" || (a.Provider == "" && a.Type == "codex"))
}

type Credentials struct{ Token, Account, Proxy string }

func hashID(id string) string { h := sha256.Sum256([]byte(id)); return hex.EncodeToString(h[:16]) }
func uuid() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("secure_random_unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
func (r *Runtime) host(method string, payload, out any) error {
	b, e := json.Marshal(payload)
	if e != nil {
		return errors.New("host_encode_failed")
	}
	raw, e := r.hostCall(method, b)
	if e != nil {
		return errors.New("host_callback_failed")
	}
	var env envelope
	if json.Unmarshal(raw, &env) != nil || !env.OK {
		return errors.New("host_callback_failed")
	}
	if out != nil && json.Unmarshal(env.Result, out) != nil {
		return errors.New("host_response_invalid")
	}
	return nil
}
func (r *Runtime) readCredentials(a AuthEntry, c Config) (Credentials, error) {
	if !a.enabled() || a.RuntimeOnly {
		return Credentials{}, errors.New("unsupported")
	}
	var d struct {
		JSON struct {
			Type       string `json:"type"`
			Disabled   bool   `json:"disabled"`
			Token      string `json:"access_token"`
			Account    string `json:"account_id"`
			AccountAlt string `json:"chatgpt_account_id"`
			Proxy      string `json:"proxy_url"`
		}
	}
	if e := r.host("host.auth.get", map[string]string{"auth_index": a.Index}, &d); e != nil {
		return Credentials{}, e
	}
	if d.JSON.Type != "codex" || d.JSON.Disabled || d.JSON.Token == "" {
		return Credentials{}, errors.New("unsupported")
	}
	account := d.JSON.Account
	if account == "" {
		account = d.JSON.AccountAlt
	}
	proxy := d.JSON.Proxy
	if proxy == "" {
		proxy = c.FallbackProxy
	}
	if proxy == "" {
		return Credentials{}, errors.New("explicit_fallback_route_required")
	}
	return Credentials{d.JSON.Token, account, proxy}, nil
}
func (r *Runtime) freshCredentials(a AuthEntry, c Config) (Credentials, error) {
	var out struct {
		Auth AuthEntry `json:"auth"`
	}
	if e := r.host("host.auth.get_runtime", map[string]string{"auth_index": a.Index}, &out); e != nil {
		return Credentials{}, e
	}
	if out.Auth.ID != a.ID || !out.Auth.enabled() {
		return Credentials{}, errors.New("credential_disabled_or_replaced")
	}
	return r.readCredentials(out.Auth, c)
}
func newHTTPClient(proxy string) (*http.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableKeepAlives = true
	if proxy != "direct" {
		u, e := url.Parse(proxy)
		if e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			return nil, errors.New("unsupported_proxy_route")
		}
		tr.Proxy = http.ProxyURL(u)
	}
	return &http.Client{Transport: tr, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect_refused") }}, nil
}

type HTTPError struct{ Code int }

func (e *HTTPError) Error() string { return fmt.Sprintf("upstream_http_%d", e.Code) }

// Probe failures are evidence about this check, not proof that the OAuth
// credential is bad. Keep the last authoritative quota snapshot usable while
// retrying transient upstream, transport, host-callback, and parse failures.
func isTransientProbeError(err error) bool {
	if err == nil {
		return false
	}
	var status *HTTPError
	if errors.As(err, &status) {
		return status.Code == 408 || status.Code == 429 || status.Code >= 500
	}
	switch err.Error() {
	case "upstream_network_error", "upstream_body_invalid", "host_callback_failed", "host_response_invalid", "callback_encode_failed":
		return true
	}
	name := err.Error()
	return strings.HasPrefix(name, "invalid_") || strings.HasPrefix(name, "quota_") || name == "codex_pool_missing"
}
func (r *Runtime) request(ctx context.Context, cr Credentials, method, path string, payload any) ([]byte, error) {
	var body []byte
	if payload != nil {
		var e error
		body, e = json.Marshal(payload)
		if e != nil {
			return nil, e
		}
	}
	client, e := newHTTPClient(cr.Proxy)
	if e != nil {
		return nil, e
	}
	defer client.CloseIdleConnections()
	req, e := http.NewRequestWithContext(ctx, method, r.baseURL+path, bytes.NewReader(body))
	if e != nil {
		return nil, errors.New("request_invalid")
	}
	req.Header.Set("Authorization", "Bearer "+cr.Token)
	if cr.Account != "" {
		req.Header.Set("ChatGPT-Account-Id", cr.Account)
	}
	req.Header.Set("User-Agent", "codex_cli_rs/0.114.0")
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := client.Do(req)
	if e != nil {
		return nil, errors.New("upstream_network_error")
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if e != nil || len(b) > 1<<20 {
		return nil, errors.New("upstream_body_invalid")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{resp.StatusCode}
	}
	return b, nil
}

const usagePath = "/backend-api/wham/usage"
const creditsPath = "/backend-api/wham/rate-limit-reset-credits"
const consumePath = creditsPath + "/consume"

func (r *Runtime) usage(ctx context.Context, c Credentials) (Snapshot, error) {
	b, e := r.request(ctx, c, "GET", usagePath, nil)
	if e != nil {
		return Snapshot{}, e
	}
	return parseUsage(b, time.Now().UTC())
}

type Credit struct {
	ID      string    `json:"id"`
	Status  string    `json:"status"`
	Expires time.Time `json:"expires_at"`
}
type CreditList struct {
	Credits   []Credit `json:"credits"`
	Available int      `json:"available_count"`
}

func (r *Runtime) credits(ctx context.Context, c Credentials) (CreditList, error) {
	b, e := r.request(ctx, c, "GET", creditsPath, nil)
	if e != nil {
		return CreditList{}, e
	}
	return parseCredits(b)
}
func parseCredits(b []byte) (CreditList, error) {
	var raw struct {
		Credits *[]struct {
			ID, Status string
			Expires    *string `json:"expires_at"`
		} `json:"credits"`
		Available *int `json:"available_count"`
	}
	if json.Unmarshal(b, &raw) != nil || raw.Credits == nil || raw.Available == nil || *raw.Available < 0 {
		return CreditList{}, errors.New("invalid_credit_response")
	}
	out := CreditList{Available: *raw.Available}
	seen := map[string]bool{}
	for _, c := range *raw.Credits {
		if c.ID == "" || seen[c.ID] {
			return out, errors.New("invalid_credit_identity")
		}
		seen[c.ID] = true
		expires := time.Time{}
		if c.Expires != nil && *c.Expires != "" {
			var e error
			expires, e = time.Parse(time.RFC3339, *c.Expires)
			if e != nil {
				return out, errors.New("invalid_credit_expiry")
			}
		}
		out.Credits = append(out.Credits, Credit{c.ID, c.Status, expires})
	}
	sort.Slice(out.Credits, func(i, j int) bool {
		if out.Credits[i].Expires.IsZero() {
			return false
		}
		if out.Credits[j].Expires.IsZero() {
			return true
		}
		return out.Credits[i].Expires.Before(out.Credits[j].Expires)
	})
	return out, nil
}
func (c CreditList) available(id string) bool {
	for _, v := range c.Credits {
		if v.ID == id && v.Status == "available" {
			return true
		}
	}
	return false
}
func excluded(c Config, id string) bool {
	for _, v := range c.Exclude {
		if strings.TrimSpace(v) == id {
			return true
		}
	}
	return false
}
