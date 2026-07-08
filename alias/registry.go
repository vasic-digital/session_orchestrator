package alias

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Alias is the registered metadata for one session alias. It holds NO credential
// material (§11.4.10) — only the stable descriptors the scheduler needs plus the
// mutable health fields. A consumer registers its own aliases by NAME only.
type Alias struct {
	Name  string // stable identity (e.g. a native or provider alias name)
	Class Class  // native (preferred) vs provider (fallback)

	// CapabilityRank orders aliases within a class by model strength / context
	// window — lower is stronger/preferred (WS-B §3.1). Native aliases use 0.
	CapabilityRank int

	// StableIndex is a fixed per-alias ordinal for deterministic tie-breaks
	// (§11.4.50). Native: numeric order; provider: an operator-declared order.
	StableIndex int

	// State is the persisted health verdict (default StateHealthy on register).
	State State

	// ExhaustedUntil is the cooldown expiry; the alias is unpickable while
	// now < ExhaustedUntil. Zero value means "no cooldown".
	ExhaustedUntil time.Time
}

// ErrDuplicate is returned by Register when an alias name is already present.
var ErrDuplicate = errors.New("alias: name already registered")

// ErrEmptyName is returned by Register when an alias name is blank.
var ErrEmptyName = errors.New("alias: name must be non-empty")

// ErrNotFound is returned when an operation targets an unregistered alias.
var ErrNotFound = errors.New("alias: name not registered")

// Registry is a concurrency-safe set of aliases keyed by name. All operations
// are safe for concurrent use; the internal lock is held only around in-memory
// map access, never across a probe or network call.
type Registry struct {
	mu      sync.RWMutex
	aliases map[string]Alias
}

// NewRegistry returns an empty registry ready for concurrent use.
func NewRegistry() *Registry {
	return &Registry{aliases: make(map[string]Alias)}
}

// Register adds an alias. A blank name is rejected (ErrEmptyName) and a repeated
// name is rejected (ErrDuplicate). A zero State defaults to StateHealthy so a
// freshly-registered alias is pickable once a live probe confirms it.
func (r *Registry) Register(a Alias) error {
	if a.Name == "" {
		return ErrEmptyName
	}
	if a.State == "" {
		a.State = StateHealthy
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.aliases[a.Name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicate, a.Name)
	}
	r.aliases[a.Name] = a
	return nil
}

// Get returns a copy of the named alias and true, or the zero Alias and false.
func (r *Registry) Get(name string) (Alias, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.aliases[name]
	return a, ok
}

// SetState updates the persisted health verdict and cooldown of an alias. Pass a
// zero exhaustedUntil to clear the cooldown. Returns ErrNotFound when absent.
func (r *Registry) SetState(name string, st State, exhaustedUntil time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.aliases[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	a.State = st
	a.ExhaustedUntil = exhaustedUntil
	r.aliases[name] = a
	return nil
}

// Names returns every registered alias name in a stable, sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.aliases))
	for n := range r.aliases {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Snapshot returns a copy of every registered alias, ordered by the WS-B §3.1
// priority key using the current wall clock for the exhaustion factor. It never
// mutates the registry. A caller that injects its own clock (the scheduler's
// cfg.Now) MUST use SnapshotAt so the candidate order is deterministic under
// that clock rather than the wall clock (§11.4.50 — ATM-680).
func (r *Registry) Snapshot() []Alias {
	return r.SnapshotAt(time.Now())
}

// SnapshotAt is Snapshot with an explicit clock: it orders the returned aliases
// by the WS-B §3.1 priority key using now for the exhaustion factor (the same
// clock the operability check uses), so a caller that injects its own clock gets
// a candidate order that is fully deterministic under that clock rather than the
// wall clock (§11.4.50). It never mutates the registry.
func (r *Registry) SnapshotAt(now time.Time) []Alias {
	r.mu.RLock()
	out := make([]Alias, 0, len(r.aliases))
	for _, a := range r.aliases {
		out = append(out, a)
	}
	r.mu.RUnlock()
	SortByPriorityAt(out, now)
	return out
}

// less reports whether alias x sorts before y under the WS-B §3.1 total order:
// (class_rank, exhaustion_rank, capability_rank, stable_index, name), all
// ascending. exhaustion_rank is derived from cooldown against the supplied now.
// Name is the final tie-break: registry names are unique (Register rejects
// duplicates via ErrDuplicate), so it is a total discriminator that makes the
// order fully deterministic regardless of map-iteration order (§11.4.50) even
// when two aliases share all four preceding keys — including StableIndex, whose
// uniqueness is not enforced at Register.
func less(x, y Alias, now time.Time) bool {
	if int(x.Class) != int(y.Class) {
		return int(x.Class) < int(y.Class)
	}
	xe, ye := exhaustionRank(x, now), exhaustionRank(y, now)
	if xe != ye {
		return xe < ye
	}
	if x.CapabilityRank != y.CapabilityRank {
		return x.CapabilityRank < y.CapabilityRank
	}
	if x.StableIndex != y.StableIndex {
		return x.StableIndex < y.StableIndex
	}
	return x.Name < y.Name
}

// exhaustionRank is 0 when the alias is free (cooldown elapsed / unset) and 1
// when it is still in cooldown (WS-B §3.1 factor 2).
func exhaustionRank(a Alias, now time.Time) int {
	if now.Before(a.ExhaustedUntil) {
		return 1
	}
	return 0
}

// SortByPriority sorts a slice of aliases in place by the WS-B §3.1 priority key
// using the current wall clock for the exhaustion factor. The order is a total
// order over stable keys, so it is deterministic across runs (§11.4.50).
func SortByPriority(as []Alias) {
	SortByPriorityAt(as, time.Now())
}

// SortByPriorityAt is SortByPriority with an explicit clock, for deterministic
// tests and reproducible scheduling decisions.
func SortByPriorityAt(as []Alias, now time.Time) {
	sort.SliceStable(as, func(i, j int) bool { return less(as[i], as[j], now) })
}

// FirstOperable walks aliases in priority order and returns the name of the
// first one for which the supplied probe function reports operable, or ("",
// false) when none is operable — an explicit, honest outcome (WS-B §4.5), never
// a silent fall-through to an unhealthy alias.
func (r *Registry) FirstOperable(now time.Time, probe func(Alias) ProbeResult) (string, bool) {
	if probe == nil {
		return "", false // no probe → nothing can be proven operable (never a panic)
	}
	// Order the candidates by the SAME injected clock the operability check uses,
	// so selection is deterministic under that clock, not the wall clock (§11.4.50
	// — ATM-680).
	for _, a := range r.SnapshotAt(now) {
		if IsOperable(a, probe(a), now) {
			return a.Name, true
		}
	}
	return "", false
}
