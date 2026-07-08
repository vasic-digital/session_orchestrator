package claim

import (
	"testing"
	"time"

	"github.com/vasic-digital/session_orchestrator/alias"
)

// healthy is a probe result that classifies HEALTHY (200 + the VERIFY_OK token).
func healthy() alias.ProbeResult {
	return alias.ProbeResult{HTTPStatus: 200, Body: "ok " + alias.VerifyToken}
}

// unhealthy is a probe result that classifies AUTH_DEAD (fail-closed).
func unhealthy() alias.ProbeResult {
	return alias.ProbeResult{HTTPStatus: 401, Body: "unauthorized"}
}

func newAliasReg(t *testing.T) *alias.Registry {
	t.Helper()
	reg := alias.NewRegistry()
	for _, a := range []alias.Alias{
		{Name: "native0", Class: alias.ClassNative, CapabilityRank: 0, StableIndex: 0},
		{Name: "native1", Class: alias.ClassNative, CapabilityRank: 0, StableIndex: 1},
		{Name: "provider0", Class: alias.ClassProvider, CapabilityRank: 0, StableIndex: 0},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register %s: %v", a.Name, err)
		}
	}
	return reg
}

func TestFirstOperableUnclaimedSkipsClaimed(t *testing.T) {
	reg := newAliasReg(t)
	cr := New(Config{})
	now := time.Now()
	allHealthy := func(alias.Alias) alias.ProbeResult { return healthy() }

	// Nothing claimed → highest-priority native0 wins.
	if got, ok := FirstOperableUnclaimed(reg, cr, now, allHealthy); !ok || got != "native0" {
		t.Fatalf("unclaimed pool: got %q,%v; want native0,true", got, ok)
	}

	// Claim native0 → selection skips it, returns next-priority native1.
	if _, out, _ := cr.TryClaim("native0", "wu-1", time.Hour); out != OutcomeGranted {
		t.Fatalf("claim native0: %q", out)
	}
	if got, ok := FirstOperableUnclaimed(reg, cr, now, allHealthy); !ok || got != "native1" {
		t.Fatalf("native0 claimed: got %q,%v; want native1,true", got, ok)
	}

	// Claim native1 too → only provider0 remains.
	cr.TryClaim("native1", "wu-2", time.Hour)
	if got, ok := FirstOperableUnclaimed(reg, cr, now, allHealthy); !ok || got != "provider0" {
		t.Fatalf("both natives claimed: got %q,%v; want provider0,true", got, ok)
	}

	// Claim everything → honest empty outcome, never a bluffed assignment.
	cr.TryClaim("provider0", "wu-3", time.Hour)
	if got, ok := FirstOperableUnclaimed(reg, cr, now, allHealthy); ok || got != "" {
		t.Fatalf("all claimed: got %q,%v; want \"\",false", got, ok)
	}
}

func TestFirstOperableUnclaimedSkipsUnhealthy(t *testing.T) {
	reg := newAliasReg(t)
	cr := New(Config{})
	now := time.Now()

	// native0 unhealthy (fail-closed) → skip to the first HEALTHY, unclaimed alias.
	probe := func(a alias.Alias) alias.ProbeResult {
		if a.Name == "native0" {
			return unhealthy()
		}
		return healthy()
	}
	if got, ok := FirstOperableUnclaimed(reg, cr, now, probe); !ok || got != "native1" {
		t.Fatalf("native0 unhealthy: got %q,%v; want native1,true", got, ok)
	}
}

func TestFirstOperableUnclaimedNilArgs(t *testing.T) {
	reg := newAliasReg(t)
	cr := New(Config{})
	now := time.Now()
	p := func(alias.Alias) alias.ProbeResult { return healthy() }

	if _, ok := FirstOperableUnclaimed(nil, cr, now, p); ok {
		t.Fatal("nil alias registry should yield false")
	}
	if _, ok := FirstOperableUnclaimed(reg, nil, now, p); ok {
		t.Fatal("nil claim registry should yield false")
	}
	if _, ok := FirstOperableUnclaimed(reg, cr, now, nil); ok {
		t.Fatal("nil probe should yield false")
	}
}
