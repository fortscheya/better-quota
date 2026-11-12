package main

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"
)

func TestRoutePriorityMutationIsRemovedFromManagementAPI(t *testing.T) {
	r, _, _ := fixture(t)
	rpcResult(t, r, "management.register", map[string]string{"BasePath": "/v0/management", "ResourceBasePath": "/v0/resource/plugins/better-quota"}, nil)
	var response managementResponse
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/management/plugins/better-quota/priorities", Body: []byte(`{"auth_id":"a","priority":1}`)}, &response)
	if response.StatusCode != 410 {
		t.Fatalf("old priority mutation status = %d", response.StatusCode)
	}
}

func TestPriorityControlsPersistConfirmAndConsumeAfterAuth(t *testing.T) {
	r, _, _ := fixture(t)
	rpcResult(t, r, "management.register", map[string]string{"BasePath": "/v0/management", "ResourceBasePath": "/v0/resource/plugins/better-quota"}, nil)
	a := r.accounts["a"]
	a.Auth.Disabled = true
	r.accounts["a"] = a
	r.publishLocked()
	var response managementResponse
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/management/plugins/better-quota/priority-controls", Body: []byte(`{"mode":"one_shot","auth_id":"a","enabled":true}`)}, &response)
	if response.StatusCode != 409 || r.cfg.OneShotPriorityAuthID != "" {
		t.Fatalf("hard unavailable target was saved without confirmation: status=%d cfg=%+v", response.StatusCode, r.cfg)
	}
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/management/plugins/better-quota/priority-controls", Body: []byte(`{"mode":"one_shot","auth_id":"a","enabled":true,"confirm_hard_unavailable":true}`)}, &response)
	if response.StatusCode != 200 || r.cfg.OneShotPriorityAuthID != "a" {
		t.Fatalf("confirmed priority target was not saved: status=%d cfg=%+v", response.StatusCode, r.cfg)
	}
	d, err := loadState(r.cfg.StatePath)
	if err != nil || d.Settings.OneShotPriorityAuthID != "a" {
		t.Fatalf("priority target was not persisted: %+v %v", d.Settings, err)
	}
	// A target remains armed after an after-auth failure, then is consumed only
	// after the same logical execution succeeds.
	a.Auth.Disabled = false
	r.accounts["a"] = a
	r.publishLocked()
	mustBegin(t, r.gate, "priority-rpc")
	p := r.gate.pick(context.Background(), pickFor("priority-rpc", "a", "alias"))
	if p.Reject || p.AuthID != "a" {
		t.Fatal(p)
	}
	a.Auth.Disabled = true
	r.accounts["a"] = a
	r.publishLocked()
	var hook HookResponse
	rpcResult(t, r, "request.intercept_after", HookRequest{RequestID: "priority-rpc", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": "a"}}, &hook)
	if !bytes.Contains(hook.ResponseBody, []byte("priority_target_unavailable")) {
		t.Fatalf("after-auth hard failure did not return priority error: %s", hook.ResponseBody)
	}
	d, err = loadState(r.cfg.StatePath)
	if err != nil || d.Settings.OneShotPriorityAuthID != "a" {
		t.Fatalf("failed after-auth consumed one-shot: %+v %v", d.Settings, err)
	}
	a.Auth.Disabled = false
	r.accounts["a"] = a
	r.publishLocked()
	if p = r.gate.pick(context.Background(), pickFor("priority-rpc", "a", "alias")); p.Reject || p.AuthID != "a" {
		t.Fatal("retry did not retain forced target", p)
	}
	rpcResult(t, r, "request.intercept_after", HookRequest{RequestID: "priority-rpc", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": "a"}}, &hook)
	d, err = loadState(r.cfg.StatePath)
	if err != nil || d.Settings.OneShotPriorityAuthID != "" || r.gate.stats().Admitted != 1 {
		t.Fatalf("successful after-auth did not consume target: %+v %v stats=%+v", d.Settings, err, r.gate.stats())
	}
	rpcResult(t, r, "request.complete", HookRequest{RequestID: "priority-rpc"}, nil)
	if s := r.gate.stats(); s.Admitted != 0 || s.Reserved != 0 {
		t.Fatalf("priority execution leaked resources: %+v", s)
	}
}

func TestBothStrategiesPublishNormalizedPriorityAndIgnoreLegacyZeros(t *testing.T) {
	for _, strategy := range []string{"capacity_weighted", "capacity_reset"} {
		t.Run(strategy, func(t *testing.T) {
			r, _, _ := fixture(t)
			r.cfg.Strategy = strategy
			r.cfg.CapacityOverrides = map[string]map[int64]float64{"alias": {18000: 0.5, 604800: 0.5}}
			r.cfg.RoutePriorityOverrides = map[string]int{"a": 0, "alias": 0}
			r.publishLocked()
			if r.accounts["a"].RoutePriority != 10000 || r.accounts["alias"].RoutePriority != 5000 || math.Abs(r.accounts["alias"].NormalizedScore-0.5) > 1e-9 {
				t.Fatal("relative scores were not normalized into actual priority")
			}
			// Force use of the next tier while the first tier is at its cap.
			for i := 0; i < r.cfg.MaxActive; i++ {
				id := string(rune('a' + i))
				mustBegin(t, r.gate, id)
				mustAdmit(t, r.gate, id, "a")
				defer r.gate.finish(id)
			}
			rows := r.dashboardAccountsLocked(r.cfg, r.gate.stats().Credentials)
			if rows[0].HighestPriority || !rows[1].HighestPriority {
				t.Fatal("full highest tier was marked preferred")
			}
			mustBegin(t, r.gate, "overflow")
			defer r.gate.finish("overflow")
			if p := r.gate.pick(context.Background(), pickFor("overflow", "a", "alias")); p.AuthID != "alias" {
				t.Fatal("scheduler did not match green tier fallback", p)
			}
		})
	}
}

func TestTransientProbeRetriesAndFatalErrorNeedsSuccessToClear(t *testing.T) {
	r, f, _ := fixture(t)
	probe := func(code int) {
		f.usageStatus = code
		r.fetchSlots <- struct{}{}
		r.jobs.Add(1)
		r.probe(context.Background(), r.accounts["a"].Auth, r.cfg)
	}
	probe(503)
	if remaining := time.Until(r.nextFetch["a"]); remaining < 4*time.Second || remaining > 5*time.Second {
		t.Fatal("first transient error did not schedule a quick retry", remaining)
	}
	probe(401)
	probe(503)
	a := r.accounts["a"]
	if a.Error != "upstream_http_401" || a.ProbeError != "upstream_http_503" || a.RoutePriority != 0 || r.gate.eligible("a", time.Now()) {
		t.Fatal("transient failure erased a confirmed credential rejection")
	}
	probe(0)
	a = r.accounts["a"]
	if a.Error != "" || a.ProbeError != "" || r.failures["a"] != 0 || !r.gate.eligible("a", time.Now()) {
		t.Fatal("successful probe did not recover credential")
	}
	// The priority mutation is only reachable through the management namespace.
	rpcResult(t, r, "management.register", map[string]string{"BasePath": "/v0/management", "ResourceBasePath": "/v0/resource/plugins/better-quota"}, nil)
	var response managementResponse
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/management/plugins/better-quota/refresh-account", Body: []byte(`{"auth_id":"a"}`)}, &response)
	if response.StatusCode != 202 {
		t.Fatalf("bad account refresh API status %d", response.StatusCode)
	}
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/resource/plugins/better-quota/refresh-account", Body: []byte(`{"auth_id":"a"}`)}, &response)
	if response.StatusCode != 404 {
		t.Fatalf("account refresh exposed as unauthenticated resource: %d", response.StatusCode)
	}
	rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: "/v0/management/plugins/better-quota/refresh-account", Body: []byte(`{"auth_id":"missing"}`)}, &response)
	if response.StatusCode != 404 {
		t.Fatalf("missing account refresh status = %d", response.StatusCode)
	}
	r.jobs.Wait()
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/v0/resource/plugins/better-quota/priorities", 404},
		{"/v0/management/plugins/better-quota/priorities", 410},
	} {
		rpcResult(t, r, "management.handle", managementRequest{Method: "POST", Path: tc.path, Body: []byte(`{"auth_id":"a","priority":0}`)}, &response)
		if response.StatusCode != tc.status {
			t.Fatalf("old priority API status at %s = %d", tc.path, response.StatusCode)
		}
	}
}
