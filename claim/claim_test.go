package claim

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// t0 is a fixed base instant so every clock-driven test is deterministic (§11.4.50).
var t0 = time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

// fixedClock returns a Clock reading *at, so a test advances time by writing *at.
func fixedClock(at *time.Time) Clock { return func() time.Time { return *at } }

// seqIDGen returns an IDGen minting c-1, c-2, … deterministically.
func seqIDGen() IDGen {
	var n uint64
	return func() string { return "c-" + itoa(atomic.AddUint64(&n, 1)) }
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

func TestTryClaimGrantsDeniesAndIsIdempotent(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()})

	c1, out, err := r.TryClaim("alias-a", "wu-1", time.Hour)
	if err != nil || out != OutcomeGranted {
		t.Fatalf("first claim: out=%q err=%v; want GRANTED, nil", out, err)
	}
	if c1.ClaimID == "" || c1.ResourceID != "alias-a" || c1.Holder != "wu-1" {
		t.Fatalf("granted claim malformed: %+v", c1)
	}

	// Different holder → clean DENIED, zero claim, NO error (never a block/panic).
	c2, out, err := r.TryClaim("alias-a", "wu-2", time.Hour)
	if err != nil {
		t.Fatalf("denied claim returned error: %v", err)
	}
	if out != OutcomeDenied {
		t.Fatalf("second holder: out=%q; want DENIED", out)
	}
	if c2 != (Claim{}) {
		t.Fatalf("denied claim not zero: %+v", c2)
	}

	// Same holder re-claim → exactly-once idempotency: same claim id, no new claim.
	c3, out, err := r.TryClaim("alias-a", "wu-1", time.Hour)
	if err != nil || out != OutcomeGrantedExisting {
		t.Fatalf("re-claim: out=%q err=%v; want GRANTED_EXISTING", out, err)
	}
	if c3.ClaimID != c1.ClaimID {
		t.Fatalf("idempotent re-claim minted a new id: %q != %q", c3.ClaimID, c1.ClaimID)
	}
	if got := r.Snapshot(); len(got) != 1 {
		t.Fatalf("live claims = %d; want exactly 1 (P1)", len(got))
	}
}

func TestTryClaimValidatesInput(t *testing.T) {
	r := New(Config{})
	cases := []struct {
		name      string
		res, hold string
		ttl       time.Duration
		wantErr   error
	}{
		{"empty resource", "", "wu", time.Second, ErrEmptyResourceID},
		{"empty holder", "a", "", time.Second, ErrEmptyHolder},
		{"zero ttl", "a", "wu", 0, ErrNonPositiveTTL},
		{"negative ttl", "a", "wu", -time.Second, ErrNonPositiveTTL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := r.TryClaim(tc.res, tc.hold, tc.ttl)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v; want %v", err, tc.wantErr)
			}
		})
	}
}

func TestReleaseRequiresMatchingClaimID(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()})
	c, _, _ := r.TryClaim("res", "wu", time.Hour)

	if err := r.Release("res", "wrong-id"); !errors.Is(err, ErrClaimMismatch) {
		t.Fatalf("mismatched release: got %v; want ErrClaimMismatch", err)
	}
	if err := r.Release("never", c.ClaimID); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("release unclaimed: got %v; want ErrNotClaimed", err)
	}
	if err := r.Release("res", c.ClaimID); err != nil {
		t.Fatalf("valid release: %v", err)
	}
	if r.IsClaimed("res") {
		t.Fatal("resource still claimed after release")
	}
}

func TestTTLReapReclaims(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()})
	if _, out, _ := r.TryClaim("res", "wu-1", 10*time.Second); out != OutcomeGranted {
		t.Fatalf("initial claim not granted: %q", out)
	}

	// Still within TTL → a competing claim is DENIED (the live claim protects it).
	at = t0.Add(5 * time.Second)
	if _, out, _ := r.TryClaim("res", "wu-2", time.Hour); out != OutcomeDenied {
		t.Fatalf("within-ttl competitor: out=%q; want DENIED", out)
	}

	// Past TTL → stale claim is reaped, resource reclaimed by the new holder.
	at = t0.Add(11 * time.Second)
	c, out, _ := r.TryClaim("res", "wu-2", time.Hour)
	if out != OutcomeGranted || c.Holder != "wu-2" {
		t.Fatalf("post-ttl reclaim: out=%q holder=%q; want GRANTED wu-2", out, c.Holder)
	}
	if !hasReap(r.Events(), "res", reasonTTLElapsed) {
		t.Fatal("no ttl_elapsed REAP event recorded")
	}
}

func TestDeadHolderReapVsLiveHolder(t *testing.T) {
	at := t0
	dead := map[string]bool{} // holder -> dead?
	r := New(Config{
		Now:      fixedClock(&at),
		NewID:    seqIDGen(),
		Liveness: func(h string) bool { return !dead[h] },
	})
	// Claim with a long TTL so only liveness can reap it.
	r.TryClaim("res", "wu-1", time.Hour)

	// Holder still alive, well within TTL → competitor DENIED.
	at = t0.Add(time.Second)
	if _, out, _ := r.TryClaim("res", "wu-2", time.Hour); out != OutcomeDenied {
		t.Fatalf("live holder: out=%q; want DENIED", out)
	}

	// Holder now provably dead → reapable before TTL, competitor GRANTED.
	dead["wu-1"] = true
	c, out, _ := r.TryClaim("res", "wu-2", time.Hour)
	if out != OutcomeGranted || c.Holder != "wu-2" {
		t.Fatalf("dead-holder reclaim: out=%q holder=%q; want GRANTED wu-2", out, c.Holder)
	}
	if !hasReap(r.Events(), "res", reasonHolderDead) {
		t.Fatal("no holder_dead REAP event recorded")
	}
}

// TestExactlyOnceUnderContention is the load-bearing §11.4.176-A proof: N
// goroutines race to claim one resource; EXACTLY ONE must win GRANTED and every
// other must get a clean DENIED (no error, no panic, no second grant). Run with
// -race to catch any lock omission. Negation verified out-of-band: removing the
// claimed-by-other check (or the lock) makes granted>1 and the assertion FAILs.
func TestExactlyOnceUnderContention(t *testing.T) {
	const n = 32
	r := New(Config{}) // real clock + default unique id gen
	var granted, denied, existing, errs int64

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		holder := "wu-" + itoa(uint64(i))
		go func() {
			defer wg.Done()
			<-start // release all goroutines at once to maximise the race
			_, out, err := r.TryClaim("hot-resource", holder, time.Hour)
			switch {
			case err != nil:
				atomic.AddInt64(&errs, 1)
			case out == OutcomeGranted:
				atomic.AddInt64(&granted, 1)
			case out == OutcomeGrantedExisting:
				atomic.AddInt64(&existing, 1)
			case out == OutcomeDenied:
				atomic.AddInt64(&denied, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if granted != 1 {
		t.Fatalf("GRANTED = %d; want EXACTLY 1 (exactly-once / P1 violated)", granted)
	}
	if existing != 0 {
		t.Fatalf("GRANTED_EXISTING = %d; want 0 (distinct holders)", existing)
	}
	if denied != n-1 {
		t.Fatalf("DENIED = %d; want %d", denied, n-1)
	}
	if errs != 0 {
		t.Fatalf("errors = %d; want 0 (denial is clean, never an error)", errs)
	}
	if got := r.Snapshot(); len(got) != 1 {
		t.Fatalf("live claims = %d; want exactly 1", len(got))
	}
}

// TestTryClaimIsNonBlocking asserts a try-claim on an already-claimed resource
// returns promptly (deadlock-free / §11.4.176-B #1), never hanging.
func TestTryClaimIsNonBlocking(t *testing.T) {
	r := New(Config{})
	r.TryClaim("res", "wu-1", time.Hour)

	done := make(chan Outcome, 1)
	go func() {
		_, out, _ := r.TryClaim("res", "wu-2", time.Hour)
		done <- out
	}()
	select {
	case out := <-done:
		if out != OutcomeDenied {
			t.Fatalf("out=%q; want DENIED", out)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TryClaim blocked on an already-claimed resource (not deadlock-free)")
	}
}

// TestDeterministicConsistency runs the SAME injected-clock/id sequence in two
// independent registries and asserts byte-identical Snapshot + Events (§11.4.50).
func TestDeterministicConsistency(t *testing.T) {
	run := func() ([]Claim, []Event) {
		at := t0
		r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()})
		r.TryClaim("a", "wu-1", 30*time.Second)
		r.TryClaim("b", "wu-2", 5*time.Second)
		r.TryClaim("a", "wu-3", time.Hour) // DENIED
		at = t0.Add(6 * time.Second)       // b's ttl elapsed
		r.TryClaim("b", "wu-3", time.Hour) // reaps b, grants wu-3
		c, _, _ := r.TryClaim("a", "wu-1", time.Hour)
		r.Release("a", c.ClaimID)
		return r.Snapshot(), r.Events()
	}
	baseSnap, baseEvents := run()
	for i := 0; i < 5; i++ {
		snap, events := run()
		if !reflect.DeepEqual(snap, baseSnap) {
			t.Fatalf("iter %d snapshot diverged:\n got %+v\nwant %+v", i, snap, baseSnap)
		}
		if !reflect.DeepEqual(events, baseEvents) {
			t.Fatalf("iter %d events diverged:\n got %+v\nwant %+v", i, events, baseEvents)
		}
	}
}

// TestSnapshotTornSafetyInMemory hammers the registry with concurrent mutators
// and readers; every Snapshot must be internally consistent (P1: no resource
// appears twice, every claim well-formed) — a reader never sees a torn state.
func TestSnapshotTornSafetyInMemory(t *testing.T) {
	r := New(Config{})
	resources := []string{"r0", "r1", "r2", "r3"}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Mutators: claim then release, churning the map.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		holder := "wu-" + itoa(uint64(w))
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, res := range resources {
					if c, out, _ := r.TryClaim(res, holder, time.Hour); out == OutcomeGranted {
						r.Release(res, c.ClaimID)
					}
				}
			}
		}()
	}
	// Readers: every snapshot must satisfy P1.
	var reads int64
	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := r.Snapshot()
				seen := map[string]bool{}
				for _, c := range snap {
					if c.ResourceID == "" || c.Holder == "" || c.ClaimID == "" {
						t.Errorf("torn/partial claim observed: %+v", c)
						return
					}
					if seen[c.ResourceID] {
						t.Errorf("P1 violated: resource %q claimed twice in one snapshot", c.ResourceID)
						return
					}
					seen[c.ResourceID] = true
				}
				atomic.AddInt64(&reads, 1)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("no snapshots were taken")
	}
}

// TestWriteStatusAtomic proves the §11.4.116 write-temp-then-rename guarantee:
// a reader concurrently reading the status file NEVER sees a half-written /
// malformed document, and every parsed snapshot is P1-consistent.
func TestWriteStatusAtomic(t *testing.T) {
	r := New(Config{})
	path := filepath.Join(t.TempDir(), "pool_status.json")
	// Seed some claims so the document is non-trivial.
	for i := 0; i < 4; i++ {
		r.TryClaim("r"+itoa(uint64(i)), "wu-"+itoa(uint64(i)), time.Hour)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // writer: republish repeatedly
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := r.WriteStatus(path); err != nil {
				t.Errorf("WriteStatus: %v", err)
				return
			}
		}
	}()

	var reads int64
	wg.Add(1)
	go func() { // reader: never observe a torn file
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			data, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				continue // pre-first-write is acceptable
			}
			if err != nil {
				t.Errorf("read status: %v", err)
				return
			}
			var st Status
			if err := json.Unmarshal(data, &st); err != nil {
				t.Errorf("torn/malformed status observed: %v", err)
				return
			}
			seen := map[string]bool{}
			for _, c := range st.Claims {
				if seen[c.ResourceID] {
					t.Errorf("P1 violated in published status: %q twice", c.ResourceID)
					return
				}
				seen[c.ResourceID] = true
			}
			atomic.AddInt64(&reads, 1)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if reads == 0 {
		t.Fatal("reader never read a complete status document")
	}
}

func TestEventsAreAppendOnlyCopies(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()})
	c, _, _ := r.TryClaim("res", "wu", time.Hour)
	r.Release("res", c.ClaimID)

	ev := r.Events()
	if len(ev) != 2 || ev[0].Kind != EventGrant || ev[1].Kind != EventRelease {
		t.Fatalf("unexpected event log: %+v", ev)
	}
	// Mutating the returned copy must not affect the registry's log.
	ev[0].Kind = "TAMPERED"
	if r.Events()[0].Kind != EventGrant {
		t.Fatal("Events() returned a live reference, not an independent copy")
	}
}

func hasReap(events []Event, resource, reason string) bool {
	for _, e := range events {
		if e.Kind == EventReap && e.ResourceID == resource && e.Reason == reason {
			return true
		}
	}
	return false
}

// TestLiveHolderPastTTLNotReaped is the SO-CLAIM-IMP-1 RED→GREEN guard
// (§11.4.115): a holder that is PROVABLY ALIVE (injected Liveness reports true)
// MUST NOT be reaped even after its TTL horizon elapses — reaping a running
// session and re-granting its exclusive resource to a second work-unit is a
// §9.2 / §11.4.176 single-owner break (two owners of one resource). RED on the
// pre-fix code (staleReason reaped on now>=expiry unconditionally): the live
// holder was reaped and wu-2 won GRANTED.
func TestLiveHolderPastTTLNotReaped(t *testing.T) {
	at := t0
	r := New(Config{
		Now:      fixedClock(&at),
		NewID:    seqIDGen(),
		Liveness: func(string) bool { return true }, // holder ALWAYS provably alive
	})
	first, out, _ := r.TryClaim("res", "wu-1", 10*time.Second)
	if out != OutcomeGranted {
		t.Fatalf("initial claim not granted: %q", out)
	}

	// Advance WELL past the TTL horizon. The holder is still provably alive, so
	// the claim MUST survive — the TTL is not a hard cap while liveness holds.
	at = t0.Add(time.Hour)

	// A competing work-unit MUST be DENIED: the live holder still owns it.
	c2, out, err := r.TryClaim("res", "wu-2", time.Hour)
	if err != nil {
		t.Fatalf("competitor claim errored: %v", err)
	}
	if out != OutcomeDenied || c2 != (Claim{}) {
		t.Fatalf("live holder past TTL was reaped: competitor out=%q claim=%+v; want DENIED, {} (§9.2 single-owner)", out, c2)
	}
	if !r.IsClaimed("res") {
		t.Fatal("live holder past TTL: IsClaimed=false; the claim was wrongly reaped")
	}
	// The live holder's own claim is unchanged and still the owner.
	snap := r.Snapshot()
	if len(snap) != 1 || snap[0].Holder != "wu-1" || snap[0].ClaimID != first.ClaimID {
		t.Fatalf("live holder claim not preserved: %+v", snap)
	}
	// No REAP event may have fired for this live holder.
	if hasReap(r.Events(), "res", reasonTTLElapsed) {
		t.Fatal("a ttl_elapsed REAP fired for a provably-live holder (§9.2 violation)")
	}
}

// TestPureTTLReapPreservedNoLiveness — (b): with NO Liveness proof configured,
// a claim past its TTL IS reaped (pure-TTL fallback preserved). The fix must not
// break the documented no-liveness lease.
func TestPureTTLReapPreservedNoLiveness(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()}) // no Liveness
	if _, out, _ := r.TryClaim("res", "wu-1", 10*time.Second); out != OutcomeGranted {
		t.Fatalf("initial claim not granted")
	}
	// Within TTL: the reap sweep must NOT reap it.
	at = t0.Add(5 * time.Second)
	if got := r.Reap(); len(got) != 0 {
		t.Fatalf("pre-expiry Reap reaped %d claims; want 0", len(got))
	}
	// Past TTL: the pure-TTL lease is reaped and reclaimable.
	at = t0.Add(11 * time.Second)
	c, out, _ := r.TryClaim("res", "wu-2", time.Hour)
	if out != OutcomeGranted || c.Holder != "wu-2" {
		t.Fatalf("pure-TTL past-expiry reclaim: out=%q holder=%q; want GRANTED wu-2", out, c.Holder)
	}
	if !hasReap(r.Events(), "res", reasonTTLElapsed) {
		t.Fatal("no ttl_elapsed REAP event for the pure-TTL lease")
	}
}

// TestDeadHolderReapedPastTTL — (c): with a Liveness proof, a claim past its TTL
// whose holder is provably DEAD IS reaped (reason holder_dead — evidence-accurate).
func TestDeadHolderReapedPastTTL(t *testing.T) {
	at := t0
	dead := map[string]bool{}
	r := New(Config{
		Now:      fixedClock(&at),
		NewID:    seqIDGen(),
		Liveness: func(h string) bool { return !dead[h] },
	})
	r.TryClaim("res", "wu-1", 10*time.Second)

	// Advance past TTL AND mark the holder dead → provably reclaimable.
	at = t0.Add(time.Hour)
	dead["wu-1"] = true
	c, out, _ := r.TryClaim("res", "wu-2", time.Hour)
	if out != OutcomeGranted || c.Holder != "wu-2" {
		t.Fatalf("dead-holder past-TTL reclaim: out=%q holder=%q; want GRANTED wu-2", out, c.Holder)
	}
	if !hasReap(r.Events(), "res", reasonHolderDead) {
		t.Fatal("no holder_dead REAP event for the dead holder past TTL")
	}
}

// TestRenewExtendsWindow — (d): Renew resets the TTL horizon so a pure-TTL claim
// survives past its ORIGINAL expiry; a competitor is DENIED inside the renewed
// window and GRANTED only once the renewed window lapses. Renew also requires the
// matching claim id (§9.2) and records a RENEW event.
func TestRenewExtendsWindow(t *testing.T) {
	at := t0
	r := New(Config{Now: fixedClock(&at), NewID: seqIDGen()}) // pure-TTL lease
	c, _, _ := r.TryClaim("res", "wu-1", 10*time.Second)      // expiry = t0+10s

	// Wrong claim id is rejected (§9.2).
	if _, err := r.Renew("res", "wrong-id"); !errors.Is(err, ErrClaimMismatch) {
		t.Fatalf("Renew with wrong id: got %v; want ErrClaimMismatch", err)
	}
	// Renew before expiry: new expiry = t0+5s+10s = t0+15s.
	at = t0.Add(5 * time.Second)
	rc, err := r.Renew("res", c.ClaimID)
	if err != nil || rc.ClaimID != c.ClaimID {
		t.Fatalf("Renew: claim=%+v err=%v; want same id, nil", rc, err)
	}

	// t0+12s is PAST the original expiry (10s) but INSIDE the renewed window (15s):
	// the claim must survive → competitor DENIED. On an un-renewed claim this
	// would have been reaped and GRANTED.
	at = t0.Add(12 * time.Second)
	if _, out, _ := r.TryClaim("res", "wu-2", time.Hour); out != OutcomeDenied {
		t.Fatalf("inside renewed window: out=%q; want DENIED (Renew did not extend)", out)
	}

	// Past the renewed window → the lease lapses. A lapsed pure-TTL lease cannot
	// be renewed (§9.2 — reap-first makes it read ErrNotClaimed, never resurrected).
	at = t0.Add(16 * time.Second)
	if _, err := r.Renew("res", c.ClaimID); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("Renew of lapsed lease: got %v; want ErrNotClaimed", err)
	}
	// And it is reclaimable by a competitor.
	if _, out, _ := r.TryClaim("res", "wu-2", time.Hour); out != OutcomeGranted {
		t.Fatalf("past renewed window: out=%q; want GRANTED", out)
	}

	// A RENEW event was recorded for the original claim.
	sawRenew := false
	for _, e := range r.Events() {
		if e.Kind == EventRenew && e.ResourceID == "res" && e.ClaimID == c.ClaimID {
			sawRenew = true
		}
	}
	if !sawRenew {
		t.Fatal("no RENEW event recorded")
	}
}
