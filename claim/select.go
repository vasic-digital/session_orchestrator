package claim

import (
	"time"

	"github.com/vasic-digital/session_orchestrator/alias"
)

// FirstOperableUnclaimed composes the alias-health layer (the sibling alias
// package — imported, never duplicated) with the claim single-owner state: it
// walks the alias registry in WS-B §3.1 priority order and returns the name of
// the first alias that is BOTH operable (per its live probe, decided fail-closed
// by alias.IsOperable — never a silent fall-through to an unhealthy alias) AND
// not currently claimed in cr. This is the WS-D §4 candidate rule "pool minus
// currently-claimed minus non-operable" — the single-owner (P1 / §11.4.119)
// filter applied on top of the fail-closed health order.
//
// It is strictly READ-ONLY over both registries: it neither claims nor mutates.
// The caller claims the returned alias via cr.TryClaim, so the check-then-claim
// race is resolved there by the atomic compare-and-set (a candidate returned
// here may lose the subsequent TryClaim to a concurrent claimant and get DENIED
// — the caller then re-selects). Returns ("", false) when no alias qualifies —
// an explicit, honest outcome (§11.4.6), never a bluffed assignment.
//
// The candidate ORDER is taken from reg.SnapshotAt(now) — the SAME injected clock
// the operability check uses — so both the exhaustion-rank sort and the cooldown
// check see one consistent instant and the candidate ordering is fully
// deterministic under an injected clock, never the wall clock (§11.4.50 —
// ATM-680).
func FirstOperableUnclaimed(reg *alias.Registry, cr *Registry, now time.Time, probe func(alias.Alias) alias.ProbeResult) (string, bool) {
	if reg == nil || cr == nil || probe == nil {
		return "", false
	}
	for _, a := range reg.SnapshotAt(now) {
		if cr.IsClaimed(a.Name) {
			continue // single-owner: already claimed by a work-unit (P1)
		}
		if alias.IsOperable(a, probe(a), now) {
			return a.Name, true
		}
	}
	return "", false
}
