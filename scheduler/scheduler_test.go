package scheduler

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vasic-digital/session_orchestrator/alias"
	"github.com/vasic-digital/session_orchestrator/claim"
)

// healthy is a probe result that classifies HEALTHY (200 + the VERIFY_OK token).
func healthy() alias.ProbeResult {
	return alias.ProbeResult{HTTPStatus: 200, Body: "ok " + alias.VerifyToken}
}

// authDead is a probe result that classifies AUTH_DEAD (fail-closed, 401).
func authDead() alias.ProbeResult {
	return alias.ProbeResult{HTTPStatus: 401, Body: "unauthorized"}
}

// allHealthy is the probe that reports every alias HEALTHY.
func allHealthy(alias.Alias) alias.ProbeResult { return healthy() }

// regWith registers the given aliases into a fresh registry, failing the test on
// any registration error.
func regWith(t *testing.T, as ...alias.Alias) *alias.Registry {
	t.Helper()
	reg := alias.NewRegistry()
	for _, a := range as {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register %s: %v", a.Name, err)
		}
	}
	return reg
}

// nativeTrio is the canonical three-alias pool (priority: native0 < native1 <
// provider0), matching the sibling packages' fixtures.
func nativeTrio(t *testing.T) *alias.Registry {
	t.Helper()
	return regWith(t,
		alias.Alias{Name: "native0", Class: alias.ClassNative, CapabilityRank: 0, StableIndex: 0},
		alias.Alias{Name: "native1", Class: alias.ClassNative, CapabilityRank: 0, StableIndex: 1},
		alias.Alias{Name: "provider0", Class: alias.ClassProvider, CapabilityRank: 0, StableIndex: 0},
	)
}

// fixedClock returns a clock that always yields the same instant (deterministic
// operability checks, §11.4.50).
func fixedClock() func() time.Time {
	t0 := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	return func() time.Time { return t0 }
}

// assertClaimState verifies the claim registry holds exactly the expected
// alias -> holder bindings — the sink-side single-owner evidence (§11.4.108):
// each expected alias is claimed by exactly the named work-unit, and the
// registry holds no other claims.
func assertClaimState(t *testing.T, cr *claim.Registry, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for _, c := range cr.Snapshot() {
		if _, dup := got[c.ResourceID]; dup {
			t.Fatalf("alias %s claimed more than once", c.ResourceID)
		}
		got[c.ResourceID] = c.Holder
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claim state=%v; want %v", got, want)
	}
}

// -----------------------------------------------------------------------------
// (a) work-units assigned to operable aliases in strict priority order.
// -----------------------------------------------------------------------------

func TestSchedulePriorityOrder(t *testing.T) {
	reg := nativeTrio(t)
	cr := claim.New(claim.Config{Now: fixedClock()})
	cfg := Config{Now: fixedClock(), Probe: allHealthy, TTL: time.Hour}

	res, err := Schedule(reg, cr, []string{"wu-A", "wu-B", "wu-C"}, cfg)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// The three work-units, in priority order, take native0, native1, provider0.
	want := []struct{ wu, aliasName string }{
		{"wu-A", "native0"},
		{"wu-B", "native1"},
		{"wu-C", "provider0"},
	}
	if len(res.Placements) != len(want) {
		t.Fatalf("placements: got %d, want %d", len(res.Placements), len(want))
	}
	for i, w := range want {
		p := res.Placements[i]
		if p.WorkUnit != w.wu || !p.Assigned || p.Alias != w.aliasName || p.ClaimID == "" || p.Existing {
			t.Fatalf("placement[%d]=%+v; want {WorkUnit:%s Alias:%s Assigned:true Existing:false ClaimID:non-empty}",
				i, p, w.wu, w.aliasName)
		}
	}
	if len(res.Unassigned) != 0 {
		t.Fatalf("unassigned: got %v, want none", res.Unassigned)
	}
	// Cross-check the claim registry state: exactly three distinct claims, each
	// alias owned by the expected work-unit (§11.4.108 sink-side evidence).
	assertClaimState(t, cr, map[string]string{
		"native0": "wu-A", "native1": "wu-B", "provider0": "wu-C",
	})
}

// -----------------------------------------------------------------------------
// (b) a non-operable alias is NEVER assigned (no-fail-open, §11.4.69).
//
// Two independent non-operability sources are exercised: a live probe that
// classifies fail-closed (401), and a persisted excluded State. In BOTH cases
// the highest-priority alias (native0) must be skipped; if the operability
// filter were removed, native0 would be claimed and this test FAILS.
// -----------------------------------------------------------------------------

func TestScheduleSkipsNonOperable(t *testing.T) {
	tests := []struct {
		name       string
		reg        func(t *testing.T) *alias.Registry
		probe      Probe
		wantAlias  string // alias wu-A must land on
		bannedName string // alias that must NEVER be claimed
	}{
		{
			name: "probe fails closed on native0",
			reg:  nativeTrio,
			probe: func(a alias.Alias) alias.ProbeResult {
				if a.Name == "native0" {
					return authDead() // fail-closed 401
				}
				return healthy()
			},
			wantAlias:  "native1",
			bannedName: "native0",
		},
		{
			name: "native0 persisted in an excluded state",
			reg: func(t *testing.T) *alias.Registry {
				// Even with a HEALTHY live probe, an excluded persisted State is
				// never operable (§11.4.69). PROVIDER_CAP is in the excluded set.
				return regWith(t,
					alias.Alias{Name: "native0", Class: alias.ClassNative, StableIndex: 0, State: alias.StateProviderCap},
					alias.Alias{Name: "native1", Class: alias.ClassNative, StableIndex: 1},
				)
			},
			probe:      allHealthy,
			wantAlias:  "native1",
			bannedName: "native0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := tc.reg(t)
			cr := claim.New(claim.Config{Now: fixedClock()})
			cfg := Config{Now: fixedClock(), Probe: tc.probe, TTL: time.Hour}

			res, err := Schedule(reg, cr, []string{"wu-A"}, cfg)
			if err != nil {
				t.Fatalf("Schedule: %v", err)
			}
			p := res.Placements[0]
			if !p.Assigned || p.Alias != tc.wantAlias {
				t.Fatalf("placement=%+v; want Assigned onto %s (skipping %s)", p, tc.wantAlias, tc.bannedName)
			}
			// The negation that makes this test load-bearing: the non-operable
			// alias must never have been claimed by anyone.
			if cr.IsClaimed(tc.bannedName) {
				t.Fatalf("non-operable alias %s was claimed — no-fail-open (§11.4.69) violated", tc.bannedName)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// (c) single-owner under concurrent scheduling (§11.4.119 / §11.4.176).
//
// N=5 work-units contend for M=2 aliases via N concurrent Schedule calls (one
// work-unit each). Assertions:
//   - exactly M=2 assigned, N-M=3 honestly Unassigned (never dropped);
//   - no alias assigned twice (verified two ways: (i) the placements report each
//     alias at most once; (ii) the claim registry holds exactly M claims on M
//     distinct aliases, each with a distinct holder);
//   - every input work-unit is accounted for exactly once.
//
// HOW THE DOUBLE-ASSIGNMENT NEGATION IS VERIFIED: if TryClaim's single-owner CAS
// were broken (e.g. two goroutines both saw native0 free and both won), two
// placements would report Alias=="native0" AND the claim registry would still be
// keyed by resource so the observable contract "each alias -> exactly one wu"
// would break in the placements. The alias->holders multimap below has size 1
// per alias exactly when single-owner holds; any list of length > 1 fails the
// test. The whole suite runs under `-race`, so a genuine data race on the shared
// registries is also reported.
// -----------------------------------------------------------------------------

func TestScheduleSingleOwnerConcurrent(t *testing.T) {
	const (
		nWorkUnits = 5
		mAliases   = 2
	)
	reg := regWith(t,
		alias.Alias{Name: "native0", Class: alias.ClassNative, StableIndex: 0},
		alias.Alias{Name: "native1", Class: alias.ClassNative, StableIndex: 1},
	)
	// Shared claim registry across all concurrent schedulers — the contended
	// single-owner resource (its default id generator is atomic/lock-safe).
	cr := claim.New(claim.Config{Now: fixedClock()})
	cfg := Config{Now: fixedClock(), Probe: allHealthy, TTL: time.Hour}

	workUnits := []string{"wu-0", "wu-1", "wu-2", "wu-3", "wu-4"}

	results := make([]Result, nWorkUnits)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, wu := range workUnits {
		wg.Add(1)
		go func(i int, wu string) {
			defer wg.Done()
			<-start // release all goroutines together → maximal CAS contention
			r, err := Schedule(reg, cr, []string{wu}, cfg)
			if err != nil {
				t.Errorf("Schedule(%s): %v", wu, err)
				return
			}
			results[i] = r
		}(i, wu)
	}
	close(start)
	wg.Wait()

	// Aggregate: build alias -> [holders] and count assigned/unassigned.
	aliasHolders := map[string][]string{}
	assigned, unassigned := 0, 0
	seenWU := map[string]bool{}
	for i, r := range results {
		if len(r.Placements) != 1 {
			t.Fatalf("result[%d]: got %d placements, want 1", i, len(r.Placements))
		}
		p := r.Placements[0]
		seenWU[p.WorkUnit] = true
		if p.Assigned {
			assigned++
			aliasHolders[p.Alias] = append(aliasHolders[p.Alias], p.WorkUnit)
		} else {
			unassigned++
		}
	}

	if len(seenWU) != nWorkUnits {
		t.Fatalf("work-units accounted for: got %d distinct, want %d (none dropped)", len(seenWU), nWorkUnits)
	}
	if assigned != mAliases || unassigned != nWorkUnits-mAliases {
		t.Fatalf("assigned=%d unassigned=%d; want assigned=%d unassigned=%d", assigned, unassigned, mAliases, nWorkUnits-mAliases)
	}
	// No alias assigned twice — the load-bearing single-owner negation.
	for aliasName, holders := range aliasHolders {
		if len(holders) != 1 {
			t.Fatalf("alias %s assigned to %v (%d holders) — single-owner (§11.4.119) violated", aliasName, holders, len(holders))
		}
	}
	if len(aliasHolders) != mAliases {
		t.Fatalf("distinct assigned aliases: got %d, want %d", len(aliasHolders), mAliases)
	}
	// Independent cross-check from the claim registry's own state.
	claims := cr.Snapshot()
	if len(claims) != mAliases {
		t.Fatalf("claim registry holds %d claims, want %d", len(claims), mAliases)
	}
	claimedAliases := map[string]bool{}
	claimHolders := map[string]bool{}
	for _, c := range claims {
		if claimedAliases[c.ResourceID] {
			t.Fatalf("alias %s claimed more than once in the registry", c.ResourceID)
		}
		claimedAliases[c.ResourceID] = true
		if claimHolders[c.Holder] {
			t.Fatalf("holder %s owns more than one alias", c.Holder)
		}
		claimHolders[c.Holder] = true
	}
}

// -----------------------------------------------------------------------------
// (d) deterministic assignment given identical inputs (§11.4.50).
//
// Two independent, identically-configured passes (fixed clock + injected
// deterministic claim-id generator) must produce byte-identical Placements,
// including claim ids.
// -----------------------------------------------------------------------------

func TestScheduleDeterministic(t *testing.T) {
	run := func() Result {
		reg := nativeTrio(t)
		var ctr int
		cr := claim.New(claim.Config{
			Now:   fixedClock(),
			NewID: func() string { ctr++; return "c-det-" + itoa(ctr) },
		})
		cfg := Config{Now: fixedClock(), Probe: allHealthy, TTL: time.Hour}
		res, err := Schedule(reg, cr, []string{"wu-A", "wu-B", "wu-C"}, cfg)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		return res
	}

	r1, r2 := run(), run()
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("non-deterministic result:\n run1=%+v\n run2=%+v", r1, r2)
	}
	// Spot-check the actual assignment is the expected deterministic one.
	if !reflect.DeepEqual(r1.Assigned, []string{"wu-A", "wu-B", "wu-C"}) {
		t.Fatalf("assigned=%v; want [wu-A wu-B wu-C]", r1.Assigned)
	}
	if r1.Placements[0].ClaimID != "c-det-1" {
		t.Fatalf("first claim id=%q; want c-det-1 (deterministic id gen)", r1.Placements[0].ClaimID)
	}
}

// -----------------------------------------------------------------------------
// Exactly-once idempotency on re-run (non-failover): a work-unit already holding
// a live claim keeps it — no new claim, no re-home.
// -----------------------------------------------------------------------------

func TestScheduleIdempotentRerun(t *testing.T) {
	reg := nativeTrio(t)
	cr := claim.New(claim.Config{Now: fixedClock()})
	cfg := Config{Now: fixedClock(), Probe: allHealthy, TTL: time.Hour}

	r1, err := Schedule(reg, cr, []string{"wu-A"}, cfg)
	if err != nil {
		t.Fatalf("Schedule#1: %v", err)
	}
	first := r1.Placements[0]
	if !first.Assigned || first.Alias != "native0" || first.Existing {
		t.Fatalf("first pass=%+v; want fresh assign onto native0", first)
	}

	r2, err := Schedule(reg, cr, []string{"wu-A"}, cfg)
	if err != nil {
		t.Fatalf("Schedule#2: %v", err)
	}
	second := r2.Placements[0]
	if !second.Assigned || second.Alias != "native0" || !second.Existing {
		t.Fatalf("second pass=%+v; want kept native0 with Existing=true", second)
	}
	if second.ClaimID != first.ClaimID {
		t.Fatalf("claim id changed across re-run: %q -> %q (exactly-once violated)", first.ClaimID, second.ClaimID)
	}
	// The registry must still hold exactly ONE claim — never a second alias for
	// the same work-unit.
	if got := cr.Snapshot(); len(got) != 1 {
		t.Fatalf("claim registry holds %d claims after re-run, want 1", len(got))
	}
}

// -----------------------------------------------------------------------------
// All-exhausted honest-block: when nothing is operable, every work-unit is
// returned Unassigned (never dropped, never a bluffed assignment, §11.4.69).
// -----------------------------------------------------------------------------

func TestScheduleAllNonOperableHonestBlock(t *testing.T) {
	reg := nativeTrio(t)
	cr := claim.New(claim.Config{Now: fixedClock()})
	allDead := func(alias.Alias) alias.ProbeResult { return authDead() }
	cfg := Config{Now: fixedClock(), Probe: allDead, TTL: time.Hour}

	res, err := Schedule(reg, cr, []string{"wu-A", "wu-B"}, cfg)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if len(res.Assigned) != 0 {
		t.Fatalf("assigned=%v; want none (all aliases non-operable)", res.Assigned)
	}
	if !reflect.DeepEqual(res.Unassigned, []string{"wu-A", "wu-B"}) {
		t.Fatalf("unassigned=%v; want [wu-A wu-B]", res.Unassigned)
	}
	for _, p := range res.Placements {
		if p.Assigned || p.Alias != "" || p.ClaimID != "" {
			t.Fatalf("placement=%+v; want honest unassigned (empty alias/claim)", p)
		}
	}
	if len(cr.Snapshot()) != 0 {
		t.Fatal("no claim must exist when nothing is operable")
	}
}

// -----------------------------------------------------------------------------
// Configuration/programming faults are rejected before any claim is made.
// -----------------------------------------------------------------------------

func TestScheduleValidation(t *testing.T) {
	reg := nativeTrio(t)
	cr := claim.New(claim.Config{Now: fixedClock()})
	good := Config{Now: fixedClock(), Probe: allHealthy, TTL: time.Hour}

	tests := []struct {
		name    string
		reg     *alias.Registry
		cr      *claim.Registry
		cfg     Config
		wus     []string
		wantErr error
	}{
		{"nil alias registry", nil, cr, good, []string{"wu"}, ErrNilAliasRegistry},
		{"nil claim registry", reg, nil, good, []string{"wu"}, ErrNilClaimRegistry},
		{"nil probe", reg, cr, Config{Now: fixedClock(), TTL: time.Hour}, []string{"wu"}, ErrNilProbe},
		{"non-positive ttl", reg, cr, Config{Now: fixedClock(), Probe: allHealthy, TTL: 0}, []string{"wu"}, ErrNonPositiveTTL},
		{"empty work-unit", reg, cr, good, []string{"wu", ""}, ErrEmptyWorkUnit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Schedule(tc.reg, tc.cr, tc.wus, tc.cfg)
			if err != tc.wantErr {
				t.Fatalf("err=%v; want %v", err, tc.wantErr)
			}
		})
	}

	// A rejected pass leaves the registries untouched (no partial claim).
	if _, err := Schedule(reg, cr, []string{"wu-A", ""}, good); err != ErrEmptyWorkUnit {
		t.Fatalf("err=%v; want ErrEmptyWorkUnit", err)
	}
	if got := cr.Snapshot(); len(got) != 0 {
		t.Fatalf("registry mutated by a rejected pass: %d claims", len(got))
	}

	// Empty work-unit list is a valid no-op, not an error.
	res, err := Schedule(reg, cr, nil, good)
	if err != nil {
		t.Fatalf("empty pass: %v", err)
	}
	if len(res.Placements) != 0 || len(res.Assigned) != 0 || len(res.Unassigned) != 0 {
		t.Fatalf("empty pass produced work: %+v", res)
	}
}

// itoa is a tiny dependency-free int->string for the deterministic id generator.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
