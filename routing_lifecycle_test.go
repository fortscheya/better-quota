package main

import (
	"context"
	"testing"
	"time"
)

func TestConsumedOneShotRetryKeepsTargetBeforeContinuous(t *testing.T) {
	c := defaultConfig()
	c.OneShotPriorityAuthID, c.ContinuousPriorityAuthID = "a", "b"
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "retry")
	if p := g.pick(context.Background(), pickFor("retry", "a", "b")); p.AuthID != "a" {
		t.Fatal(p)
	}
	mustAdmit(t, g, "retry", "a")
	if !g.consumeOneShot("retry", "a") {
		t.Fatal("not consumed")
	}
	if p := g.pick(context.Background(), pickFor("retry", "a", "b")); p.AuthID != "a" {
		t.Fatalf("one-shot retry moved to continuous target: %+v", p)
	}
	g.finish("retry")
}

func TestPrioritySurvivesReservationExpiryAndSameAuthUpgrade(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		t.Run(map[bool]string{false: "expired", true: "upgrade"}[upgrade], func(t *testing.T) {
			c := defaultConfig()
			c.MaxActive = 1
			g := setupGate(c, "a")
			now := time.Now()
			g.now = func() time.Time { return now }
			mustBegin(t, g, "forced")
			if upgrade {
				if p := g.pick(context.Background(), pickFor("forced", "a")); p.AuthID != "a" {
					t.Fatal(p)
				}
			}
			c.ContinuousPriorityAuthID = "a"
			g.update(c, g.accounts)
			if p := g.pick(context.Background(), pickFor("forced", "a")); p.AuthID != "a" {
				t.Fatal(p)
			}
			if !upgrade {
				now = now.Add(11 * time.Second)
			}
			mustBegin(t, g, "occupied")
			if !upgrade {
				mustAdmit(t, g, "occupied", "a")
			}
			a := g.accounts["a"]
			a.Quota.Windows[0].Used = 95
			a.Score = 0
			g.accounts["a"] = a
			if err := g.admit("forced", "a"); err != nil {
				t.Fatalf("priority lost before after-auth: %v", err)
			}
			g.finish("forced")
			g.finish("occupied")
			if s := g.stats(); s.Admitted != 0 || s.Reserved != 0 {
				t.Fatal(s)
			}
		})
	}
}

func TestInternalPingDoesNotConsumeExternalPriority(t *testing.T) {
	c := defaultConfig()
	g := setupGate(c, "a", "b")
	mustBegin(t, g, "ping")
	mustAdmit(t, g, "ping", "a")
	if !g.pinInternal("ping", "a") {
		t.Fatal("pin failed")
	}
	c.OneShotPriorityAuthID = "b"
	g.update(c, g.accounts)
	req := pickFor("", "a")
	req.Options.Headers = nil
	req.Options.Metadata = map[string]any{"source": "plugin_host_model_callback", "pinned_auth_id": "a"}
	if p := g.pick(context.Background(), req); p.AuthID != "a" {
		t.Fatalf("external priority hijacked pinned auto-ping: %+v", p)
	}
	if _, claimed := g.priorityClaim("ping"); claimed {
		t.Fatal("ping claimed external one-shot")
	}
	g.finish("ping")
}
