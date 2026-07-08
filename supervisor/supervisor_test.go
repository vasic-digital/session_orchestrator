package supervisor

import (
	"sync"
	"testing"
	"time"
)

// base is a fixed instant so every test is fully deterministic (§11.4.50) — no
// wall-clock is ever read (the `now` passed to Check IS the injected clock).
var base = time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

const (
	window = 60 * time.Second // LivenessWindow
	grace  = 30 * time.Second // GraceWindow (SUSPECT band)
)

// staticProber returns a fixed liveness verdict for every entity — an injected
// proof (§11.4.28) whose behaviour a test controls exactly.
func staticProber(v Liveness) Prober { return func(string) Liveness { return v } }

// -----------------------------------------------------------------------------
// Classify — the pure analyzer, table-driven over the required cases plus the
// SUSPECT band and boundary conditions (§11.4.107(10) golden-good/golden-bad).
// -----------------------------------------------------------------------------

func TestClassify_Table(t *testing.T) {
	cases := []struct {
		name       string
		ageSeconds int // now − lastSeen, in seconds
		proof      Liveness
		want       State
		wantReason Reason
	}{
		// (a) fresh heartbeat, no proof → ALIVE.
		{"a_fresh_no_proof", 10, LivenessUnknown, StateAlive, ReasonHeartbeatFresh},
		// (b) stale heartbeat (past window+grace), no proof → DEAD (honest).
		{"b_stale_no_proof_dead", 120, LivenessUnknown, StateDead, ReasonHeartbeatStaleDead},
		// (c) stale heartbeat, proof ALIVE → ALIVE (liveness authoritative past window).
		{"c_stale_proof_alive", 120, LivenessAlive, StateAlive, ReasonProofAlive},
		// (d) fresh heartbeat, proof DEAD → DEAD (liveness authoritative within window).
		{"d_fresh_proof_dead", 5, LivenessDead, StateDead, ReasonProofDead},
		// SUSPECT band: stale within grace (window < age < window+grace), no proof.
		{"suspect_within_grace", 75, LivenessUnknown, StateSuspect, ReasonHeartbeatStaleSuspect},
		// SUSPECT is overridden by an authoritative ALIVE proof.
		{"suspect_proof_alive", 75, LivenessAlive, StateAlive, ReasonProofAlive},
		// SUSPECT is overridden by an authoritative DEAD proof.
		{"suspect_proof_dead", 75, LivenessDead, StateDead, ReasonProofDead},
		// Boundary: age exactly == window → stale (not fresh) → SUSPECT (grace > 0).
		{"boundary_at_window", 60, LivenessUnknown, StateSuspect, ReasonHeartbeatStaleSuspect},
		// Boundary: age exactly == window+grace → past grace → DEAD.
		{"boundary_at_window_plus_grace", 90, LivenessUnknown, StateDead, ReasonHeartbeatStaleDead},
		// Negative age (future last-seen / clock skew) → fresh → ALIVE.
		{"negative_age_future", -5, LivenessUnknown, StateAlive, ReasonHeartbeatFresh},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lastSeen := base.Add(-time.Duration(tc.ageSeconds) * time.Second)
			got, reason := Classify(lastSeen, base, window, grace, tc.proof)
			if got != tc.want {
				t.Fatalf("Classify state = %q, want %q", got, tc.want)
			}
			if reason != tc.wantReason {
				t.Fatalf("Classify reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestClassify_C_FalseDeathNegation is the explicit NEGATION verification for
// case (c): it proves the ALIVE-proof branch is load-bearing. The heartbeat is
// DEFINITELY stale-past-grace, so under pure heartbeat-age logic (the proof
// ignored) the entity WOULD be classified DEAD — a FALSE DEATH. The test asserts
// (1) the real verdict WITH the ALIVE proof is ALIVE, and (2) the identical inputs
// WITHOUT the proof (UNKNOWN) classify DEAD. If the liveness-respect branch were
// deleted/broken, assertion (1) would fall through to the age logic → DEAD → this
// test FAILS. That is the mutation this test catches.
func TestClassify_C_FalseDeathNegation(t *testing.T) {
	lastSeen := base.Add(-120 * time.Second) // stale past window+grace (90s)

	// (2) Establish the counterfactual: without the proof this is DEAD.
	if got, _ := Classify(lastSeen, base, window, grace, LivenessUnknown); got != StateDead {
		t.Fatalf("precondition: age-only classification = %q, want DEAD "+
			"(the heartbeat must be stale so the alive-proof is the ONLY thing that saves it)", got)
	}

	// (1) With the ALIVE proof the entity must NOT be declared dead (liveness
	// authoritative, §11.4.6). Broken liveness-respect → DEAD → assertion fails.
	got, reason := Classify(lastSeen, base, window, grace, LivenessAlive)
	if got == StateDead {
		t.Fatalf("FALSE DEATH: alive-proof ignored — stale heartbeat classified DEAD despite proof of life")
	}
	if got != StateAlive || reason != ReasonProofAlive {
		t.Fatalf("liveness-respect broken: got (%q,%q), want (ALIVE,proof_alive)", got, reason)
	}
}

// -----------------------------------------------------------------------------
// Supervisor.Check — the stateful watchdog over the same required cases.
// -----------------------------------------------------------------------------

func newSup(t *testing.T, cfg Config) *Supervisor {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func verdictFor(t *testing.T, vs []Verdict, id string) Verdict {
	t.Helper()
	for _, v := range vs {
		if v.Entity == id {
			return v
		}
	}
	t.Fatalf("no verdict for entity %q", id)
	return Verdict{}
}

func TestCheck_A_FreshAlive(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace})
	if err := s.Register("orch", base.Add(-10*time.Second)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	v := verdictFor(t, s.Check(base), "orch")
	if v.State != StateAlive {
		t.Fatalf("fresh heartbeat = %q, want ALIVE", v.State)
	}
}

func TestCheck_B_StaleNoProofDead(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: 0}) // no grace → stale == DEAD
	if err := s.Register("orch", base.Add(-120*time.Second)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	v := verdictFor(t, s.Check(base), "orch")
	if v.State != StateDead {
		t.Fatalf("stale heartbeat + no proof = %q, want DEAD", v.State)
	}
	if got := s.DeadSet(base); len(got) != 1 || got[0] != "orch" {
		t.Fatalf("DeadSet = %v, want [orch]", got)
	}
}

func TestCheck_C_StaleProofAliveNotDead(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace, Prober: staticProber(LivenessAlive)})
	if err := s.Register("orch", base.Add(-120*time.Second)); err != nil { // stale past grace
		t.Fatalf("Register: %v", err)
	}
	v := verdictFor(t, s.Check(base), "orch")
	if v.State == StateDead {
		t.Fatalf("FALSE DEATH: stale entity with proof-of-life classified DEAD")
	}
	if v.State != StateAlive {
		t.Fatalf("stale heartbeat + proof ALIVE = %q, want ALIVE (liveness authoritative)", v.State)
	}
	if got := s.DeadSet(base); len(got) != 0 {
		t.Fatalf("DeadSet = %v, want empty (proof of life)", got)
	}
}

func TestCheck_D_ProofDeadWithinWindow(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace, Prober: staticProber(LivenessDead)})
	if err := s.Register("orch", base.Add(-5*time.Second)); err != nil { // fresh heartbeat
		t.Fatalf("Register: %v", err)
	}
	v := verdictFor(t, s.Check(base), "orch")
	if v.State != StateDead {
		t.Fatalf("fresh heartbeat + proof DEAD = %q, want DEAD (proof authoritative within window)", v.State)
	}
}

func TestCheck_SuspectBand(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace})
	if err := s.Register("orch", base.Add(-75*time.Second)); err != nil { // window < age < window+grace
		t.Fatalf("Register: %v", err)
	}
	v := verdictFor(t, s.Check(base), "orch")
	if v.State != StateSuspect {
		t.Fatalf("stale-within-grace = %q, want SUSPECT", v.State)
	}
	if got := s.DeadSet(base); len(got) != 0 {
		t.Fatalf("SUSPECT must not be in DeadSet, got %v", got)
	}
}

// TestCheck_HeartbeatRevivesEntity proves a fresh heartbeat moves a would-be-dead
// entity back to ALIVE — the consumer-supplied liveness signal is honored.
func TestCheck_HeartbeatRevivesEntity(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: 0})
	if err := s.Register("orch", base.Add(-120*time.Second)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v := verdictFor(t, s.Check(base), "orch"); v.State != StateDead {
		t.Fatalf("pre-heartbeat = %q, want DEAD", v.State)
	}
	if err := s.Heartbeat("orch", base); err != nil { // seen now
		t.Fatalf("Heartbeat: %v", err)
	}
	if v := verdictFor(t, s.Check(base), "orch"); v.State != StateAlive {
		t.Fatalf("post-heartbeat = %q, want ALIVE", v.State)
	}
}

// TestHeartbeat_MonotonicForward proves an out-of-order (older) heartbeat is
// ignored — the freshest evidence wins (§11.4.6).
func TestHeartbeat_MonotonicForward(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: 0})
	if err := s.Register("orch", base); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := s.Heartbeat("orch", base.Add(-1*time.Hour)); err != nil { // stale, must be ignored
		t.Fatalf("Heartbeat: %v", err)
	}
	if ls, _ := s.LastSeen("orch"); !ls.Equal(base) {
		t.Fatalf("older heartbeat overwrote last-seen: got %v, want %v", ls, base)
	}
}

// -----------------------------------------------------------------------------
// (e) Determinism across N iterations (§11.4.50).
// -----------------------------------------------------------------------------

func TestCheck_DeterministicAcrossIterations(t *testing.T) {
	const iterations = 100
	build := func() *Supervisor {
		s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace, Prober: staticProber(LivenessUnknown)})
		mustReg := func(id string, ageS int) {
			if err := s.Register(id, base.Add(-time.Duration(ageS)*time.Second)); err != nil {
				t.Fatalf("Register %s: %v", id, err)
			}
		}
		mustReg("a__alive", 10)
		mustReg("b__suspect", 75)
		mustReg("c__dead", 200)
		return s
	}

	// A reference run.
	ref := build().Check(base)
	// Every fresh supervisor over identical inputs must yield an identical verdict
	// slice (same order, same states, same reasons, same ages) — and the same
	// DeadSet — across N iterations.
	for i := 0; i < iterations; i++ {
		s := build()
		got := s.Check(base)
		if len(got) != len(ref) {
			t.Fatalf("iter %d: len=%d, want %d", i, len(got), len(ref))
		}
		for j := range got {
			if got[j] != ref[j] {
				t.Fatalf("iter %d: verdict[%d]=%+v, want %+v", i, j, got[j], ref[j])
			}
		}
		dead := s.DeadSet(base)
		if len(dead) != 1 || dead[0] != "c__dead" {
			t.Fatalf("iter %d: DeadSet=%v, want [c__dead]", i, dead)
		}
	}
}

// -----------------------------------------------------------------------------
// (f) Concurrent Check / Heartbeat are race-safe (run under -race).
// -----------------------------------------------------------------------------

func TestConcurrent_CheckAndHeartbeat(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: grace, Prober: staticProber(LivenessAlive)})
	const n = 64
	for i := 0; i < n; i++ {
		if err := s.Register(entityID(i), base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Concurrent Checkers.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				now := base.Add(time.Duration(k) * time.Millisecond)
				vs := s.Check(now)
				_ = s.DeadSet(now)
				if len(vs) != n {
					t.Errorf("Check returned %d verdicts, want %d", len(vs), n)
					return
				}
			}
		}(g)
	}
	// Concurrent Heartbeaters.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 500; k++ {
				_ = s.Heartbeat(entityID((g*13+k)%n), base.Add(time.Duration(k)*time.Millisecond))
			}
		}(g)
	}

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Final consistency: every entity still classified (proof is ALIVE → all ALIVE).
	vs := s.Check(base.Add(time.Hour))
	if len(vs) != n {
		t.Fatalf("final Check returned %d verdicts, want %d", len(vs), n)
	}
	for _, v := range vs {
		if v.State != StateAlive {
			t.Fatalf("entity %q = %q, want ALIVE (proof of life)", v.Entity, v.State)
		}
	}
}

func entityID(i int) string {
	return "atmosphere__entity__" + string(rune('A'+i%26)) + string(rune('0'+i/26))
}

// -----------------------------------------------------------------------------
// Event log, config validation, registry errors.
// -----------------------------------------------------------------------------

func TestCheck_TransitionEvents(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: 0})
	if err := s.Register("orch", base); err != nil { // fresh → will be ALIVE
		t.Fatalf("Register: %v", err)
	}
	// First check: transition "" → ALIVE.
	s.Check(base)
	// Second check well past the window: transition ALIVE → DEAD.
	s.Check(base.Add(10 * time.Minute))
	// Third check at the same later instant: NO new transition (still DEAD).
	s.Check(base.Add(10 * time.Minute))

	var transitions []Event
	for _, e := range s.Events() {
		if e.Kind == EventTransition {
			transitions = append(transitions, e)
		}
	}
	if len(transitions) != 2 {
		t.Fatalf("transition events = %d, want 2 (\"\"→ALIVE, ALIVE→DEAD)", len(transitions))
	}
	if transitions[0].From != "" || transitions[0].To != StateAlive {
		t.Fatalf("transition[0] = %+v, want (\"\"→ALIVE)", transitions[0])
	}
	if transitions[1].From != StateAlive || transitions[1].To != StateDead {
		t.Fatalf("transition[1] = %+v, want (ALIVE→DEAD)", transitions[1])
	}
}

// TestDeadSet_DoesNotEmitEvents proves DeadSet is a read-only peek (§ owns no log).
func TestDeadSet_DoesNotEmitEvents(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window, GraceWindow: 0})
	if err := s.Register("orch", base.Add(-200*time.Second)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	before := len(s.Events())
	for i := 0; i < 5; i++ {
		s.DeadSet(base)
	}
	if after := len(s.Events()); after != before {
		t.Fatalf("DeadSet emitted events: before=%d after=%d", before, after)
	}
}

func TestNew_ConfigValidation(t *testing.T) {
	if _, err := New(Config{LivenessWindow: 0, GraceWindow: grace}); err != ErrNonPositiveWindow {
		t.Fatalf("New(window=0) err = %v, want ErrNonPositiveWindow", err)
	}
	if _, err := New(Config{LivenessWindow: window, GraceWindow: -1}); err != ErrNegativeGrace {
		t.Fatalf("New(grace<0) err = %v, want ErrNegativeGrace", err)
	}
	if _, err := New(Config{LivenessWindow: window}); err != nil {
		t.Fatalf("New(valid) err = %v, want nil", err)
	}
}

func TestRegistry_Errors(t *testing.T) {
	s := newSup(t, Config{LivenessWindow: window})
	if err := s.Register("", base); err != ErrEmptyEntity {
		t.Fatalf("Register(\"\") = %v, want ErrEmptyEntity", err)
	}
	if err := s.Register("x", base); err != nil {
		t.Fatalf("Register(x): %v", err)
	}
	if err := s.Register("x", base); err == nil {
		t.Fatalf("duplicate Register(x) = nil, want ErrDuplicate")
	}
	if err := s.Heartbeat("missing", base); err == nil {
		t.Fatalf("Heartbeat(missing) = nil, want ErrNotFound")
	}
	if err := s.Remove("missing"); err == nil {
		t.Fatalf("Remove(missing) = nil, want ErrNotFound")
	}
	if err := s.Remove("x"); err != nil {
		t.Fatalf("Remove(x): %v", err)
	}
	if got := s.Entities(); len(got) != 0 {
		t.Fatalf("Entities after remove = %v, want empty", got)
	}
}

func TestLiveness_String(t *testing.T) {
	for l, want := range map[Liveness]string{
		LivenessUnknown: "UNKNOWN", LivenessAlive: "ALIVE", LivenessDead: "DEAD",
	} {
		if got := l.String(); got != want {
			t.Fatalf("Liveness(%d).String() = %q, want %q", l, got, want)
		}
	}
}
