package alias

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Alias{Name: "native0", Class: ClassNative}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	a, ok := r.Get("native0")
	if !ok {
		t.Fatal("Get native0: not found")
	}
	if a.State != StateHealthy {
		t.Fatalf("default State = %q; want HEALTHY", a.State)
	}
}

func TestRegisterRejectsEmptyAndDuplicate(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Alias{Name: ""}); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("empty name: got %v; want ErrEmptyName", err)
	}
	if err := r.Register(Alias{Name: "dup"}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := r.Register(Alias{Name: "dup"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate: got %v; want ErrDuplicate", err)
	}
}

func TestSetState(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(Alias{Name: "p"})
	until := t0.Add(time.Hour)
	if err := r.SetState("p", StateProviderCap, until); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	a, _ := r.Get("p")
	if a.State != StateProviderCap || !a.ExhaustedUntil.Equal(until) {
		t.Fatalf("SetState not applied: %+v", a)
	}
	if err := r.SetState("missing", StateHealthy, time.Time{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetState missing: got %v; want ErrNotFound", err)
	}
}

// TestSortByPriority asserts the WS-B §3.1 total order: native before provider,
// free before in-cooldown, stronger capability before weaker, then stable index.
func TestSortByPriority(t *testing.T) {
	cooled := t0.Add(time.Minute)
	in := []Alias{
		{Name: "prov_strong", Class: ClassProvider, CapabilityRank: 1, StableIndex: 10},
		{Name: "native_b", Class: ClassNative, CapabilityRank: 0, StableIndex: 2},
		{Name: "native_a", Class: ClassNative, CapabilityRank: 0, StableIndex: 1},
		{Name: "native_cooling", Class: ClassNative, CapabilityRank: 0, StableIndex: 0, ExhaustedUntil: cooled},
		{Name: "prov_weak", Class: ClassProvider, CapabilityRank: 5, StableIndex: 3},
	}
	SortByPriorityAt(in, t0)
	got := make([]string, len(in))
	for i, a := range in {
		got[i] = a.Name
	}
	want := []string{"native_a", "native_b", "native_cooling", "prov_strong", "prov_weak"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order[%d] = %q; want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestSortByPriorityDeterministic asserts §11.4.50: identical input yields an
// identical order every time.
func TestSortByPriorityDeterministic(t *testing.T) {
	mk := func() []Alias {
		return []Alias{
			{Name: "c", Class: ClassProvider, CapabilityRank: 2, StableIndex: 3},
			{Name: "a", Class: ClassNative, StableIndex: 1},
			{Name: "b", Class: ClassNative, StableIndex: 2},
		}
	}
	first := mk()
	SortByPriorityAt(first, t0)
	for iter := 0; iter < 5; iter++ {
		next := mk()
		SortByPriorityAt(next, t0)
		for i := range first {
			if first[i].Name != next[i].Name {
				t.Fatalf("iter %d nondeterministic at %d: %q vs %q", iter, i, first[i].Name, next[i].Name)
			}
		}
	}
}

// TestFirstOperable proves selection returns the first priority-ordered operable
// alias and returns ("",false) — never a fall-through — when none is operable.
func TestFirstOperable(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(Alias{Name: "native_capped", Class: ClassNative, StableIndex: 1, State: StateProviderCap})
	_ = r.Register(Alias{Name: "native_ok", Class: ClassNative, StableIndex: 2})
	_ = r.Register(Alias{Name: "provider_ok", Class: ClassProvider, StableIndex: 3})

	healthy := func(a Alias) ProbeResult { return ProbeResult{HTTPStatus: 200, Body: VerifyToken} }
	name, ok := r.FirstOperable(t0, healthy)
	if !ok || name != "native_ok" {
		t.Fatalf("FirstOperable = (%q,%v); want (native_ok,true)", name, ok)
	}

	dead := func(a Alias) ProbeResult { return ProbeResult{Err: errTimeout} }
	if name, ok := r.FirstOperable(t0, dead); ok || name != "" {
		t.Fatalf("all-dead FirstOperable = (%q,%v); want (\"\",false)", name, ok)
	}
}

// TestRegistryConcurrent exercises the lock under -race with mixed readers and
// writers on the shared registry.
func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 8; i++ {
		_ = r.Register(Alias{Name: string(rune('a' + i)), StableIndex: i})
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := string(rune('a' + i%8))
			for j := 0; j < 200; j++ {
				if j%2 == 0 {
					_ = r.SetState(name, StateHealthy, time.Time{})
				} else {
					_, _ = r.Get(name)
					_ = r.Snapshot()
					_ = r.Names()
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestSnapshotDeterministicTieBreak drives the REAL Register→Snapshot (map-backed)
// path — not a literal slice — with eight aliases sharing ALL FOUR primary sort
// keys (class, exhaustion, capability, stableIndex), so ONLY the Name tie-break
// discriminates them. It asserts Snapshot() yields a byte-identical, name-ascending
// order across N iterations. Pre-fix (no Name tie-break) the tied aliases retain
// Go's randomised map-iteration order, so a specific-order assertion FAILs
// (§11.4.115 RED-on-broken-artifact); post-fix the Name tie-break makes the order
// total + deterministic regardless of map iteration, so it PASSes (§11.4.50).
func TestSnapshotDeterministicTieBreak(t *testing.T) {
	r := NewRegistry()
	// Registered in DESCENDING name order; all four primary keys are identical,
	// so registration order is irrelevant and only Name can break the tie.
	for _, n := range []string{"h", "g", "f", "e", "d", "c", "b", "a"} {
		if err := r.Register(Alias{Name: n, Class: ClassNative, CapabilityRank: 0, StableIndex: 0}); err != nil {
			t.Fatalf("Register %q: %v", n, err)
		}
	}
	want := []string{"a", "b", "c", "d", "e", "f", "g", "h"} // name-ascending tie-break

	const iters = 32
	var firstOrder []string
	for iter := 0; iter < iters; iter++ {
		snap := r.Snapshot()
		got := make([]string, len(snap))
		for i, a := range snap {
			got[i] = a.Name
		}
		// (a) specific-order assertion — the pre-fix code cannot guarantee this.
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("iter %d: order[%d] = %q; want name-ascending %q (full: %v)", iter, i, got[i], want[i], got)
			}
		}
		// (b) byte-identical-across-iterations assertion — the determinism claim.
		if firstOrder == nil {
			firstOrder = got
			continue
		}
		for i := range firstOrder {
			if got[i] != firstOrder[i] {
				t.Fatalf("iter %d nondeterministic at %d: %q vs first-run %q", iter, i, got[i], firstOrder[i])
			}
		}
	}
}

// TestFirstOperableNilProbe proves a nil probe func is a clean ("",false) outcome,
// never a panic (the reviewer's MINOR guard).
func TestFirstOperableNilProbe(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Alias{Name: "n", Class: ClassNative}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	name, ok := r.FirstOperable(t0, nil)
	if ok || name != "" {
		t.Fatalf("nil probe = (%q,%v); want (\"\",false)", name, ok)
	}
}
