// Package scheduler implements the non-failover ASSIGNMENT layer of the
// session_orchestrator engine (WS-D DESIGN §4): it places a set of work-units
// onto the flowing alias pool, giving each work-unit the highest-priority
// OPERABLE alias and CLAIMING it exactly-once so no two work-units ever share an
// alias (single-owner, P1 / §11.4.119).
//
// It is a pure COMPOSITION of the two sibling packages — it imports and
// orchestrates their exported primitives, and duplicates none of their internals:
//   - alias.Registry.Snapshot / alias.IsOperable — the WS-B §3.1 priority order +
//     the fail-closed operability predicate (via claim.FirstOperableUnclaimed).
//   - claim.FirstOperableUnclaimed — the candidate rule "pool minus
//     currently-claimed minus non-operable", walked in priority order.
//   - claim.TryClaim — the atomic, non-blocking, exactly-once compare-and-set
//     that makes single-owner true even under concurrent schedulers (§11.4.176).
//
// Scope boundary (§11.4.6 / task mandate): this is the NON-FAILOVER path only.
// The same-session orchestrator failover/resume spine (WS-D §2 — capture
// session-id → detect limit → quiesce → atomic swap → `claude --resume` on a new
// alias) is UNCONFIRMED / POC-gated and is deliberately NOT implemented here. A
// work-unit that already holds a live claim is kept idempotently (exactly-once);
// it is never re-homed onto a different alias, even if its current alias has
// degraded — that re-homing IS the WS-C float and lives elsewhere.
//
// Decoupling contract (§11.4.28 / §11.4.177): this package hardcodes NO track,
// alias name, directory, ttl, threshold, or project string. Work-unit ids are
// opaque consumer-supplied strings (the consumer track-qualifies them per
// §11.4.178 — e.g. "atmosphere__track3__work"); the clock, the health probe, and
// the claim ttl are all injected (Config). It holds NO credential material
// (§11.4.10) — the caller performs each probe and hands the engine only the
// observable result.
//
// Anti-bluff contract (§11.4.69 / no-fail-open): a non-operable alias is NEVER
// assigned; when no operable alias can be claimed for a work-unit the scheduler
// returns an explicit, honest UNASSIGNED placement — never a silent drop and
// never a bluffed assignment onto an unhealthy alias. Every input work-unit
// appears exactly once in the result (the never-drop guarantee).
package scheduler

import (
	"errors"
	"time"

	"github.com/vasic-digital/session_orchestrator/alias"
	"github.com/vasic-digital/session_orchestrator/claim"
)

// Probe is the live health-probe function the consumer supplies (§11.4.28): it
// performs the actual request against an alias's endpoint — passing any key via
// env/config, never on the command line (§11.4.10) — and returns only the
// observable result the engine classifies. It must be safe for concurrent use.
type Probe func(alias.Alias) alias.ProbeResult

// Config carries the injected dependencies for one scheduling pass.
type Config struct {
	// Now is the injected clock (§11.4.50): it is sampled ONCE at the start of a
	// Schedule call and that single instant is used for every operability
	// (cooldown) check in the pass, so the pass sees one consistent point in time
	// and identical inputs yield identical assignments. A nil Now uses the real
	// wall clock.
	Now func() time.Time
	// Probe is the live health probe (required). A nil Probe is a configuration
	// error — there is no path that treats "no probe" as "everything healthy"
	// (§11.4.69 no-fail-open).
	Probe Probe
	// TTL is the reap horizon applied to every claim minted in the pass. It must
	// be positive (a holder keeps a long-lived claim alive via claim.Renew or a
	// Liveness proof configured on the claim registry).
	TTL time.Duration
}

// Placement is the outcome for exactly one work-unit. Every input work-unit
// produces exactly one Placement (never dropped, §11.4.69).
type Placement struct {
	// WorkUnit is the opaque consumer-supplied work-unit id (echoed verbatim).
	WorkUnit string
	// Alias is the alias name claimed for this work-unit; "" when Unassigned.
	Alias string
	// ClaimID is the exactly-once claim id bound to (WorkUnit → Alias); "" when
	// Unassigned.
	ClaimID string
	// Assigned is true when an operable alias was claimed for this work-unit.
	Assigned bool
	// Existing is true when the work-unit already held a live claim before this
	// pass and that claim was kept idempotently (exactly-once — no new claim, no
	// re-home). It is meaningful only when Assigned is true.
	Existing bool
}

// Result is the full outcome of a scheduling pass.
type Result struct {
	// Placements holds one Placement per input work-unit, in the input (priority)
	// order — the never-drop audit trail.
	Placements []Placement
	// Assigned lists the ids of work-units that obtained an alias (convenience,
	// input order).
	Assigned []string
	// Unassigned lists the ids of work-units for which no operable alias could be
	// claimed — the explicit honest-block set (§11.4.69), in input order.
	Unassigned []string
}

var (
	// ErrNilAliasRegistry is returned when the alias registry argument is nil.
	ErrNilAliasRegistry = errors.New("scheduler: alias registry must be non-nil")
	// ErrNilClaimRegistry is returned when the claim registry argument is nil.
	ErrNilClaimRegistry = errors.New("scheduler: claim registry must be non-nil")
	// ErrNilProbe is returned when Config.Probe is nil — there is no fail-open
	// default that assumes an unprobed alias is healthy (§11.4.69).
	ErrNilProbe = errors.New("scheduler: Config.Probe must be non-nil (no fail-open)")
	// ErrNonPositiveTTL is returned when Config.TTL is <= 0.
	ErrNonPositiveTTL = errors.New("scheduler: Config.TTL must be positive")
	// ErrEmptyWorkUnit is returned when a work-unit id is blank.
	ErrEmptyWorkUnit = errors.New("scheduler: work-unit id must be non-empty")
)

// Schedule places workUnits (given in priority order — most-critical first, per
// §11.4.72 / §11.4.132; the caller establishes the order) onto the flowing pool.
// For each work-unit, in order, it claims the highest-priority operable alias
// that is not already claimed, exactly-once. A work-unit already holding a live
// claim keeps it (idempotent). A work-unit with no claimable operable alias is
// returned Unassigned (never dropped, never double-assigned).
//
// It is deterministic (§11.4.50): with a fixed injected clock and a pure probe,
// identical inputs over identically-configured registries yield an identical
// Result. Under concurrent Schedule calls the only nondeterminism is which
// caller wins a contended alias's compare-and-set; single-owner is preserved
// regardless (the loser re-selects the next candidate in the same total order).
//
// The returned error is non-nil only for a configuration/programming fault
// (nil registry, nil probe, non-positive TTL, empty work-unit id) detected
// before any claim is made — so a returned error leaves the registries
// untouched.
func Schedule(reg *alias.Registry, cr *claim.Registry, workUnits []string, cfg Config) (Result, error) {
	if reg == nil {
		return Result{}, ErrNilAliasRegistry
	}
	if cr == nil {
		return Result{}, ErrNilClaimRegistry
	}
	if cfg.Probe == nil {
		return Result{}, ErrNilProbe
	}
	if cfg.TTL <= 0 {
		return Result{}, ErrNonPositiveTTL
	}
	for _, wu := range workUnits {
		if wu == "" {
			return Result{}, ErrEmptyWorkUnit
		}
	}

	now := time.Now
	if cfg.Now != nil {
		now = cfg.Now
	}
	// Sample the clock ONCE so the whole pass sees one consistent instant
	// (§11.4.50 determinism).
	at := now()

	res := Result{Placements: make([]Placement, 0, len(workUnits))}
	for _, wu := range workUnits {
		p := placeOne(reg, cr, wu, at, cfg.Probe, cfg.TTL)
		res.Placements = append(res.Placements, p)
		if p.Assigned {
			res.Assigned = append(res.Assigned, wu)
		} else {
			res.Unassigned = append(res.Unassigned, wu)
		}
	}
	return res, nil
}

// placeOne places a single work-unit. It first honors exactly-once idempotency
// (a work-unit already holding a live claim keeps it — non-failover, never
// re-homed), then selects and claims the highest-priority operable unclaimed
// alias.
func placeOne(reg *alias.Registry, cr *claim.Registry, wu string, at time.Time, probe Probe, ttl time.Duration) Placement {
	// Exactly-once (§11.4.176-A): if this work-unit already owns a live claim,
	// keep it — do NOT claim a second alias for it (which would give one holder
	// two aliases) and do NOT re-home it onto a different alias (that is the WS-C
	// float, out of scope, §11.4.6).
	if existing, ok := heldBy(cr, wu); ok {
		return Placement{WorkUnit: wu, Alias: existing.ResourceID, ClaimID: existing.ClaimID, Assigned: true, Existing: true}
	}

	// Candidate loop. Each iteration composes the sibling primitive
	// claim.FirstOperableUnclaimed (highest-priority, IsOperable-passing,
	// unclaimed alias) and claims it via claim.TryClaim's atomic CAS. The loop is
	// bounded by the alias count: a DENIED means a CONCURRENT scheduler won that
	// candidate, so the next FirstOperableUnclaimed call sees it claimed and
	// returns a DIFFERENT candidate — the candidate pool strictly shrinks, so the
	// loop makes progress and terminates (§11.4.176-B ordered-acquisition: the
	// loser takes the next candidate in the same WS-B total order).
	maxAttempts := len(reg.Names()) + 1
	for attempt := 0; attempt < maxAttempts; attempt++ {
		name, ok := claim.FirstOperableUnclaimed(reg, cr, at, probe)
		if !ok {
			return Placement{WorkUnit: wu} // honest UNASSIGNED — no operable alias
		}
		c, outcome, err := cr.TryClaim(name, wu, ttl)
		if err != nil {
			// Unreachable given the upfront validation (name is a registered
			// non-empty alias, wu is validated non-empty, ttl is validated
			// positive); returning UNASSIGNED here is the honest, non-bluff
			// fallback rather than fabricating an assignment.
			return Placement{WorkUnit: wu}
		}
		switch outcome {
		case claim.OutcomeGranted, claim.OutcomeGrantedExisting:
			return Placement{WorkUnit: wu, Alias: name, ClaimID: c.ClaimID, Assigned: true}
		case claim.OutcomeDenied:
			continue // a concurrent scheduler won this alias → re-select next candidate
		}
	}
	// Pool churned past the attempt budget (adversarial claim/release cycling by
	// out-of-band holders) → honest UNASSIGNED, never a bluffed assignment.
	return Placement{WorkUnit: wu}
}

// heldBy returns the live claim a work-unit already owns, if any. Uses the
// O(1) ClaimByHolder reverse-index lookup (holderIndex map) instead of the
// O(n) Snapshot-based scan for faster scheduling with many aliases (§11.4.141).
// ClaimByHolder reaps stale claims first, so an expired/dead-holder claim
// reads as absent — only a genuinely-live claim is honored (§11.4.6).
func heldBy(cr *claim.Registry, wu string) (claim.Claim, bool) {
	return cr.ClaimByHolder(wu)
}
