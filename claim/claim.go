// Package claim implements the flowing-pool CLAIM REGISTRY of the
// session_orchestrator engine (WS-D DESIGN §1.3–§1.4): the exactly-once,
// deadlock-free, single-owner assignment registry that binds an exclusive
// resource (an alias — one Claude-Code CLI credential/session context — or any
// work-item) to at most one holder (work-unit) at a time.
//
// It is the operational instantiation of the constitution's §11.4.176
// (exactly-once claim + deadlock-free device-lock), composing §11.4.119
// (single-resource-owner), §11.4.116 (append-only event log + atomically
// snapshotted status), §11.4.180 (TTL / dead-holder reap), and §11.4.50
// (deterministic under injected clock + id generator).
//
// Decoupling contract (§11.4.28 / §11.4.177): this package hardcodes NO track,
// alias name, directory, path, ttl, or project string. Resource ids and holder
// ids are opaque consumer-supplied strings; the clock, the claim-id generator,
// and the holder-liveness proof are all injected (Config). It never reaches for
// `/mnt/trackN`, `claude1..N`, or any project asset, and it holds NO credential
// material (§11.4.10) — only the observable claim bookkeeping.
//
// Anti-bluff contract (§11.4.6 / §11.4.176 / WS-D DESIGN §1.4#3): "the TTL
// elapsed" and "the holder is dead" are decided from evidence, never guessed. A
// claim is reaped ONLY when the holder is provably reclaimable:
//   - When a Liveness proof is supplied it is authoritative: the claim is reaped
//     when (and only when) that proof reports the holder provably DEAD. A
//     provably-ALIVE holder is NEVER reaped, EVEN PAST ITS TTL — reaping a
//     running session would cross-contaminate it and hand its exclusive resource
//     to a second owner (§9.2). The live proof is the "heartbeat"; the holder
//     extends the TTL window explicitly via Renew.
//   - When NO Liveness proof is supplied it is a pure-TTL lease: liveness cannot
//     be proven, so the claim is reaped once now >= its expiry (the documented
//     fallback). Such a holder MUST Renew before expiry to keep the claim.
//
// Absence of a liveness proof never becomes "assume dead"; presence of a live
// proof never becomes "reap on TTL anyway".
//
// Scope boundary (§11.4.6): this registry is the claim spine only. The
// same-session orchestrator failover/resume protocol (WS-D §2) is UNCONFIRMED /
// POC-gated and is deliberately NOT implemented here.
package claim

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Clock returns the current time. It is injected so scheduling decisions and the
// TTL/reap horizon are reproducible in tests and across runs (§11.4.50).
type Clock func() time.Time

// IDGen mints a unique claim id per granted binding. It is injected so tests are
// deterministic (§11.4.50); the default is a per-registry monotonic counter.
type IDGen func() string

// Liveness reports whether the holder of a claim is PROVABLY alive. It is
// injected (§11.4.28): the consumer supplies its own proof — `kill -0` on the
// holder's supervising pid, a heartbeat-file mtime, etc. It MUST be a fast,
// non-blocking local check (no network, no probe) because it runs under the
// registry lock (WS-D §1.4: the lock is held only for the compare-and-set +
// reap, microseconds, never across a network call). A nil Liveness means
// liveness cannot be proven → claims are reaped by TTL only (§11.4.6 — never
// assume a holder is dead).
type Liveness func(holder string) bool

// Claim is one exactly-once binding of a resource to a holder.
type Claim struct {
	ResourceID string        `json:"resource_id"` // the exclusive resource (e.g. alias name)
	Holder     string        `json:"holder"`      // the work-unit that owns it
	ClaimID    string        `json:"claim_id"`    // unique, minted once per binding
	GrantedAt  time.Time     `json:"granted_at"`  // instant the claim was granted
	TTL        time.Duration `json:"ttl"`         // reap horizon relative to GrantedAt
}

// ExpiresAt is the instant at and after which the claim is TTL-reapable.
func (c Claim) ExpiresAt() time.Time { return c.GrantedAt.Add(c.TTL) }

// Outcome is the (never-erroring) verdict of a TryClaim attempt.
type Outcome string

const (
	// OutcomeGranted — the resource was free and is now claimed by the caller.
	OutcomeGranted Outcome = "GRANTED"
	// OutcomeGrantedExisting — the caller already held a live claim on the
	// resource; the existing claim is returned UNCHANGED (exactly-once
	// idempotency). Re-claiming does NOT refresh the TTL window — a holder keeping
	// a long-lived claim alive MUST call Renew (or supply a Liveness proof, which
	// keeps the claim alive implicitly while the holder is provably alive).
	OutcomeGrantedExisting Outcome = "GRANTED_EXISTING"
	// OutcomeDenied — the resource is live-claimed by a DIFFERENT holder; a
	// clean rejection (never an error, never a block).
	OutcomeDenied Outcome = "DENIED"
)

// EventKind labels an entry in the append-only audit spine (§11.4.116).
type EventKind string

const (
	EventGrant         EventKind = "GRANT"
	EventGrantExisting EventKind = "GRANT_EXISTING"
	EventDeny          EventKind = "DENY"
	EventRelease       EventKind = "RELEASE"
	EventRenew         EventKind = "RENEW"
	EventReap          EventKind = "REAP"
)

// Event is one immutable audit record. The log is append-only, never rewritten.
type Event struct {
	At         time.Time `json:"ts"`
	Kind       EventKind `json:"event"`
	ResourceID string    `json:"resource"`
	Holder     string    `json:"holder,omitempty"`
	ClaimID    string    `json:"claim_id,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

// Reap reasons (closed set, evidence-named per §11.4.6).
const (
	reasonTTLElapsed = "ttl_elapsed"
	reasonHolderDead = "holder_dead"
	reasonClaimedBy  = "claimed_by_other"
)

var (
	// ErrEmptyResourceID is returned when a resource id is blank.
	ErrEmptyResourceID = errors.New("claim: resource id must be non-empty")
	// ErrEmptyHolder is returned when a holder id is blank.
	ErrEmptyHolder = errors.New("claim: holder must be non-empty")
	// ErrNonPositiveTTL is returned when a claim ttl is <= 0.
	ErrNonPositiveTTL = errors.New("claim: ttl must be positive")
	// ErrNotClaimed is returned by Release when the resource holds no live claim.
	ErrNotClaimed = errors.New("claim: resource is not currently claimed")
	// ErrClaimMismatch is returned by Release when the claim id does not match
	// the live claim — a stale holder cannot release a claim that was already
	// reaped and re-granted to someone else (§9.2).
	ErrClaimMismatch = errors.New("claim: claim id does not match the live claim")
)

// Config carries the injected dependencies. All fields are optional; nil fields
// fall back to safe defaults (real wall clock, per-registry monotonic id
// generator, TTL-only reaping).
type Config struct {
	Now      Clock
	NewID    IDGen
	Liveness Liveness
}

// Registry is the concurrency-safe claim registry. At most one live claim exists
// per resource id (invariant P1 / §11.4.119). Every mutating decision is made
// under a single lock held only for the in-memory compare-and-set + reap — never
// across file I/O or any injected callback that could block.
type Registry struct {
	mu     sync.Mutex
	claims map[string]Claim
	events []Event
	now    Clock
	newID  IDGen
	alive  Liveness
}

// New returns an empty registry ready for concurrent use.
func New(cfg Config) *Registry {
	r := &Registry{
		claims: make(map[string]Claim),
		now:    cfg.Now,
		newID:  cfg.NewID,
		alive:  cfg.Liveness,
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.newID == nil {
		var ctr uint64
		r.newID = func() string { return "c-" + strconv.FormatUint(atomic.AddUint64(&ctr, 1), 10) }
	}
	return r
}

// TryClaim attempts to claim resourceID for holder with the given ttl.
//
// NON-BLOCKING (§11.4.176-B #1): it returns immediately — GRANTED (resource was
// free), GRANTED_EXISTING (caller already held it), or DENIED (a different
// holder holds it) — and never waits holding a partial claim.
//
// ALL-OR-NOTHING / single-owner (§11.4.176-B #2, P1 / §11.4.119): the check
// "is resourceID free?" and the write "claim it" are one atomic step under the
// lock (compare-and-set on absence). Two concurrent claimants for the same free
// resource are serialized by the lock: exactly ONE wins GRANTED, every other
// gets a clean DENIED — they can never both see it free and both win.
//
// EXACTLY-ONCE (§11.4.176-A): re-claiming a resource the SAME holder already
// holds is idempotent — the existing claim id is returned (GRANTED_EXISTING),
// never a second claim.
//
// TTL / dead-holder reap (§11.4.176-B #3): stale claims are reaped before the
// decision, so a resource whose prior holder's TTL elapsed or is provably dead
// is claimable again.
func (r *Registry) TryClaim(resourceID, holder string, ttl time.Duration) (Claim, Outcome, error) {
	if resourceID == "" {
		return Claim{}, "", ErrEmptyResourceID
	}
	if holder == "" {
		return Claim{}, "", ErrEmptyHolder
	}
	if ttl <= 0 {
		return Claim{}, "", ErrNonPositiveTTL
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.reapStaleLocked(now)

	if existing, held := r.claims[resourceID]; held {
		if existing.Holder == holder {
			r.events = append(r.events, Event{
				At: now, Kind: EventGrantExisting, ResourceID: resourceID,
				Holder: holder, ClaimID: existing.ClaimID,
			})
			return existing, OutcomeGrantedExisting, nil
		}
		r.events = append(r.events, Event{
			At: now, Kind: EventDeny, ResourceID: resourceID,
			Holder: holder, Reason: reasonClaimedBy,
		})
		return Claim{}, OutcomeDenied, nil
	}

	c := Claim{ResourceID: resourceID, Holder: holder, ClaimID: r.newID(), GrantedAt: now, TTL: ttl}
	r.claims[resourceID] = c
	r.events = append(r.events, Event{
		At: now, Kind: EventGrant, ResourceID: resourceID, Holder: c.Holder, ClaimID: c.ClaimID,
	})
	return c, OutcomeGranted, nil
}

// Release frees the live claim on resourceID. The caller MUST pass the claim id
// it was granted: a mismatched id is rejected (ErrClaimMismatch) so a stale
// holder cannot release a claim that has since been reaped and re-granted to a
// different holder (§9.2). ErrNotClaimed is returned when nothing is claimed.
func (r *Registry) Release(resourceID, claimID string) error {
	if resourceID == "" {
		return ErrEmptyResourceID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, held := r.claims[resourceID]
	if !held {
		return ErrNotClaimed
	}
	if c.ClaimID != claimID {
		return ErrClaimMismatch
	}
	delete(r.claims, resourceID)
	r.events = append(r.events, Event{
		At: r.now(), Kind: EventRelease, ResourceID: resourceID, Holder: c.Holder, ClaimID: c.ClaimID,
	})
	return nil
}

// Renew is the explicit heartbeat: it extends the TTL window of the live claim on
// resourceID by resetting GrantedAt to now, so the claim survives past its
// original expiry. It is the documented way a holder keeps a long-lived claim
// alive under the pure-TTL lease (no Liveness proof configured); with a Liveness
// proof the claim is already kept alive implicitly while the holder is provably
// alive, and Renew simply re-anchors the horizon.
//
// Like Release, the caller MUST pass the claim id it was granted: a mismatched id
// is rejected (ErrClaimMismatch) so a stale holder cannot renew a claim that has
// since been reaped and re-granted to a different holder (§9.2). Stale claims are
// reaped BEFORE the lookup, so a pure-TTL lease that already lapsed reads as
// ErrNotClaimed — a holder MUST renew before expiry; a lapsed lease is never
// silently resurrected (§9.2 / §11.4.176). The renewed claim is returned.
func (r *Registry) Renew(resourceID, claimID string) (Claim, error) {
	if resourceID == "" {
		return Claim{}, ErrEmptyResourceID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.reapStaleLocked(now)
	c, held := r.claims[resourceID]
	if !held {
		return Claim{}, ErrNotClaimed
	}
	if c.ClaimID != claimID {
		return Claim{}, ErrClaimMismatch
	}
	c.GrantedAt = now
	r.claims[resourceID] = c
	r.events = append(r.events, Event{
		At: now, Kind: EventRenew, ResourceID: resourceID, Holder: c.Holder, ClaimID: c.ClaimID,
	})
	return c, nil
}

// IsClaimed reports whether resourceID currently holds a live claim, reaping any
// stale claim first so an expired/dead-holder resource reads as free.
func (r *Registry) IsClaimed(resourceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapStaleLocked(r.now())
	_, held := r.claims[resourceID]
	return held
}

// Reap sweeps every resource and reaps stale claims (TTL elapsed or holder
// provably dead), returning the reaped claims in deterministic resource-id
// order (§11.4.50). A live-held claim is never reaped.
func (r *Registry) Reap() []Claim {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reapStaleLocked(r.now())
}

// reapStaleLocked removes every stale claim under the held lock, appending one
// REAP event per reap in deterministic (sorted resource-id) order so the event
// log is reproducible across runs (§11.4.50). Callers MUST hold r.mu.
func (r *Registry) reapStaleLocked(now time.Time) []Claim {
	if len(r.claims) == 0 {
		return nil
	}
	ids := make([]string, 0, len(r.claims))
	for id := range r.claims {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var reaped []Claim
	for _, id := range ids {
		c := r.claims[id]
		reason, stale := r.staleReason(c, now)
		if !stale {
			continue
		}
		delete(r.claims, id)
		r.events = append(r.events, Event{
			At: now, Kind: EventReap, ResourceID: id, Holder: c.Holder, ClaimID: c.ClaimID, Reason: reason,
		})
		reaped = append(reaped, c)
	}
	return reaped
}

// staleReason decides, from evidence, whether a claim is reapable and why
// (§11.4.6 / SO-CLAIM-IMP-1 / WS-D DESIGN §1.4#3: reap on "TTL elapsed WITH NO
// HEARTBEAT", never on TTL alone while the holder is provably alive).
//
//   - With a Liveness proof configured the proof is authoritative at ALL times:
//     a provably-DEAD holder is reaped (reason holder_dead) whether before OR
//     after expiry (the fast-path reclaim, preserved), and a provably-ALIVE
//     holder is NEVER reaped, EVEN PAST ITS TTL — reaping a running session and
//     re-granting its exclusive resource to a second work-unit would be a §9.2 /
//     §11.4.176 single-owner break. The live proof IS the heartbeat; the holder
//     extends the window explicitly via Renew.
//   - With NO Liveness proof (pure-TTL lease) liveness cannot be proven, so the
//     claim is reaped once its TTL horizon elapses (reason ttl_elapsed) — the
//     documented fallback. Such a holder MUST Renew before expiry to keep it.
func (r *Registry) staleReason(c Claim, now time.Time) (string, bool) {
	if r.alive != nil {
		// Liveness is authoritative: dead → reap (any time); alive → never reap,
		// even past TTL. Absence of proof-of-death is never "assume dead" (§11.4.6).
		if !r.alive(c.Holder) {
			return reasonHolderDead, true
		}
		return "", false
	}
	// Pure-TTL lease: no liveness proof → reap once the TTL horizon elapses.
	if !now.Before(c.ExpiresAt()) { // now >= expiry
		return reasonTTLElapsed, true
	}
	return "", false
}

// Snapshot returns a complete, internally-consistent copy of every live claim,
// ordered by resource id, after reaping stale claims. Because the copy is taken
// under the lock, a concurrent reader never observes a torn/partial state
// (§11.4.116).
func (r *Registry) Snapshot() []Claim {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reapStaleLocked(r.now())
	out := make([]Claim, 0, len(r.claims))
	for _, c := range r.claims {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceID < out[j].ResourceID })
	return out
}

// Events returns an independent copy of the append-only audit spine. Mutating
// the returned slice never affects the registry.
func (r *Registry) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// Status is the serialized, atomically-published view written by WriteStatus.
type Status struct {
	GeneratedAt time.Time `json:"generated_at"`
	Claims      []Claim   `json:"claims"`
}

// WriteStatus publishes the current claim set to path atomically (§11.4.116
// write-temp-then-rename): the JSON is written to a sibling temp file and then
// os.Rename'd over path, so a concurrent reader of path always sees either the
// previous complete document or the new complete document — never a half-written
// file. The marshal + write happen OUTSIDE the registry lock (the mutex is never
// held across file I/O). The temp file is created in path's directory so the
// rename is a same-filesystem atomic replace.
func (r *Registry) WriteStatus(path string) (err error) {
	st := Status{GeneratedAt: r.now(), Claims: r.Snapshot()}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		// Best-effort cleanup: a successful rename already moved the temp away,
		// so this Remove is a no-op; on any error path it deletes the stray temp.
		if rmErr := os.Remove(tmpName); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) && err == nil {
			err = rmErr
		}
	}()

	if _, werr := tmp.Write(data); werr != nil {
		tmp.Close()
		return werr
	}
	if serr := tmp.Sync(); serr != nil {
		tmp.Close()
		return serr
	}
	if cerr := tmp.Close(); cerr != nil {
		return cerr
	}
	return os.Rename(tmpName, path)
}
