// Package supervisor implements the WS-D §2.3 SUPERVISOR / WATCHDOG of the
// session_orchestrator engine: the external DEATH-DETECTION layer that watches a
// set of monitored entities (the floating orchestrator ROLE and/or pool aliases)
// and, on each Check, classifies every entity ALIVE / SUSPECT / DEAD from two
// sources of evidence — the age of its consumer-supplied heartbeat/last-seen
// timestamp against a configured liveness window, AND an injectable liveness
// proof.
//
// Why a watchdog at all (cited lesson, WS-D DESIGN §2.3 / WS-E §4.4): "the
// checkpointer saves state, but there is no automatic failure detection. If your
// process crashes, no one knows. There is no supervisor, no watchdog, no
// heartbeat." A durable on-disk assignment table is NECESSARY but NOT SUFFICIENT
// — something separate from the orchestrator's own alias must DETECT that the
// orchestrator/alias approached a limit or died and SIGNAL it. This package is
// that detector.
//
// Signal, never act (WS-D DESIGN §2.3 / §11.4.6): the supervisor does NOT itself
// perform recovery. It emits an explicit, honest verdict per entity and returns
// the DEAD set so a caller / recovery path (e.g. the claim registry's TTL / dead-
// holder reap per §11.4.180, or the WS-C float) acts. It never silently reaps,
// re-homes, or fabricates aliveness.
//
// Liveness is AUTHORITATIVE, in BOTH directions, when it can render a verdict —
// mirroring the claim registry's liveness-respecting semantics (claim.staleReason
// / SO-CLAIM-IMP-1 / §11.4.6): a proof reporting DEAD declares the entity DEAD
// even while its heartbeat is still fresh; a proof reporting ALIVE keeps the
// entity ALIVE even after its heartbeat window has elapsed (the live proof IS the
// heartbeat). Only when the proof is UNKNOWN — no proof configured, or a probe
// that could not conclude — does heartbeat age decide. Absence of proof-of-death
// never becomes "assume dead" and absence of proof-of-life never overrides a
// proof that the entity is alive.
//
// Decoupling contract (§11.4.28 / §11.4.177): this package hardcodes NO track,
// alias name, directory, window, threshold, or project string. Entity ids are
// opaque consumer-supplied strings (the consumer track-qualifies them per
// §11.4.178 — e.g. "atmosphere__orchestrator__role"); the heartbeat timestamps,
// the classification windows, the current instant (`now` on every Check — the
// injected clock, §11.4.50), and the liveness proof are ALL supplied by the
// consumer. It holds NO credential material (§11.4.10) — only the observable
// liveness bookkeeping.
//
// Anti-bluff contract (§11.4.6 / §11.4.69 no-fail-open): classification is a
// total, pure function of the evidence — see Classify, exported so a self-
// validation harness can exercise the analyzer directly with golden-good and
// golden-bad inputs (§11.4.107(10)). No path converts absence of evidence into
// "healthy": with no live proof, a heartbeat past the window classifies SUSPECT
// (within grace) then DEAD (past grace) — never a silent ALIVE.
//
// Scope boundary (§11.4.6): this is the DETECTION spine only. The same-session
// orchestrator failover/resume protocol (WS-D §2.2 — checkpoint → select → WS-C
// atomic credential swap → `claude --resume` on a new alias) is UNCONFIRMED /
// POC-gated and is deliberately NOT implemented here.
package supervisor

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// State is the liveness classification of one monitored entity.
type State string

const (
	// StateAlive — the entity is live: its heartbeat is fresh (within the
	// liveness window) OR its liveness proof reports it provably alive.
	StateAlive State = "ALIVE"
	// StateSuspect — the entity's heartbeat is stale (past the liveness window)
	// but still within the grace band, and no proof has resolved it either way.
	// It is not yet declared dead — the honest intermediate verdict.
	StateSuspect State = "SUSPECT"
	// StateDead — the entity is presumed/ proven dead: its liveness proof reports
	// it dead, OR (no proof) its heartbeat is stale past the grace band. This is
	// the set the caller acts on (reap / bootstrap-replacement), never done here.
	StateDead State = "DEAD"
)

// Liveness is the three-valued verdict an injected proof returns. It is three-
// valued (unlike the claim registry's binary kill-0 proof) because a real probe
// can genuinely be inconclusive, and §11.4.6 forbids turning "cannot tell" into a
// guess — an UNKNOWN proof falls back to heartbeat-age evidence, never to a
// fabricated verdict.
type Liveness int

const (
	// LivenessUnknown — the proof could not determine liveness (no proof
	// configured, probe unreachable/timed out). Classification falls back to
	// heartbeat age. It is NEVER read as "alive" or "dead" (§11.4.6).
	LivenessUnknown Liveness = iota
	// LivenessAlive — the proof PROVES the entity alive. Authoritative: keeps the
	// entity ALIVE even past its heartbeat window.
	LivenessAlive
	// LivenessDead — the proof PROVES the entity dead. Authoritative: declares the
	// entity DEAD even while its heartbeat is still fresh.
	LivenessDead
)

// String renders a Liveness verdict for evidence/logging.
func (l Liveness) String() string {
	switch l {
	case LivenessAlive:
		return "ALIVE"
	case LivenessDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// Prober is the injected liveness proof (§11.4.28). Given an entity id it returns
// a three-valued liveness verdict. It runs UNDER the supervisor lock (like the
// claim registry's Liveness), so it MUST be a fast, non-blocking LOCAL check (no
// network, no long probe) and MUST NOT call back into the Supervisor (which would
// deadlock). A nil Prober means no proof is available → every entity is
// classified by heartbeat age alone.
type Prober func(entity string) Liveness

// Reason is the closed-set evidence label naming WHY a verdict was reached
// (§11.4.6 — honest, named, never a guess).
type Reason string

const (
	// ReasonProofDead — a liveness proof reported the entity provably dead.
	ReasonProofDead Reason = "proof_dead"
	// ReasonProofAlive — a liveness proof reported the entity provably alive.
	ReasonProofAlive Reason = "proof_alive"
	// ReasonHeartbeatFresh — no proof verdict; heartbeat age is within the window.
	ReasonHeartbeatFresh Reason = "heartbeat_fresh"
	// ReasonHeartbeatStaleSuspect — no proof verdict; heartbeat is past the window
	// but within the grace band.
	ReasonHeartbeatStaleSuspect Reason = "heartbeat_stale_suspect"
	// ReasonHeartbeatStaleDead — no proof verdict; heartbeat is past the grace band.
	ReasonHeartbeatStaleDead Reason = "heartbeat_stale_dead"
)

// Classify is the pure, total classification analyzer (§11.4.107(10) — self-
// validatable with golden-good/golden-bad inputs). Given an entity's last-seen
// instant, the current instant, the liveness/grace windows, and the liveness
// proof verdict, it returns exactly one State plus the evidence Reason.
//
// The liveness proof is AUTHORITATIVE in BOTH directions when it can render a
// verdict (mirrors claim.staleReason, §11.4.6):
//   - LivenessDead → DEAD, even while the heartbeat is still fresh.
//   - LivenessAlive → ALIVE, even after the heartbeat window has elapsed.
//
// Only when the proof is LivenessUnknown does heartbeat age decide:
//   - age < livenessWindow                       → ALIVE   (fresh)
//   - livenessWindow ≤ age < livenessWindow+grace → SUSPECT (stale, within grace)
//   - age ≥ livenessWindow+grace                  → DEAD    (stale, past grace)
//
// A negative age (a last-seen in the future under clock skew) is < livenessWindow
// and so classifies ALIVE — an honest reading of "definitely recent".
func Classify(lastSeen, now time.Time, livenessWindow, graceWindow time.Duration, proof Liveness) (State, Reason) {
	switch proof {
	case LivenessDead:
		return StateDead, ReasonProofDead
	case LivenessAlive:
		return StateAlive, ReasonProofAlive
	}
	age := now.Sub(lastSeen)
	if age < livenessWindow {
		return StateAlive, ReasonHeartbeatFresh
	}
	if age < livenessWindow+graceWindow {
		return StateSuspect, ReasonHeartbeatStaleSuspect
	}
	return StateDead, ReasonHeartbeatStaleDead
}

// Verdict is the per-entity outcome of one Check.
type Verdict struct {
	// Entity is the opaque consumer-supplied entity id (echoed verbatim).
	Entity string `json:"entity"`
	// State is the classification (ALIVE / SUSPECT / DEAD).
	State State `json:"state"`
	// Reason is the evidence label for the verdict (§11.4.6).
	Reason Reason `json:"reason"`
	// LastSeen is the heartbeat instant the verdict was computed against.
	LastSeen time.Time `json:"last_seen"`
	// Age is now − LastSeen at the Check instant.
	Age time.Duration `json:"age"`
	// Proof is the liveness-proof verdict consulted for this entity.
	Proof Liveness `json:"proof"`
}

// EventKind labels an entry in the append-only audit spine (§11.4.116).
type EventKind string

const (
	// EventRegister — an entity was registered for monitoring.
	EventRegister EventKind = "REGISTER"
	// EventRemove — an entity was deregistered.
	EventRemove EventKind = "REMOVE"
	// EventTransition — an entity's classification changed (including its first
	// observation, recorded as a transition from the empty state).
	EventTransition EventKind = "TRANSITION"
)

// Event is one immutable audit record. The log is append-only, never rewritten
// (§11.4.116). Verdict transitions are the load-bearing signal a conductor tails.
type Event struct {
	At     time.Time `json:"ts"`
	Kind   EventKind `json:"event"`
	Entity string    `json:"entity"`
	From   State     `json:"from,omitempty"`
	To     State     `json:"to,omitempty"`
	Reason Reason    `json:"reason,omitempty"`
}

var (
	// ErrEmptyEntity is returned when an entity id is blank.
	ErrEmptyEntity = errors.New("supervisor: entity id must be non-empty")
	// ErrDuplicate is returned by Register when an entity id is already monitored.
	ErrDuplicate = errors.New("supervisor: entity id already registered")
	// ErrNotFound is returned when an operation targets an unmonitored entity.
	ErrNotFound = errors.New("supervisor: entity id not registered")
	// ErrNonPositiveWindow is returned by New when LivenessWindow <= 0.
	ErrNonPositiveWindow = errors.New("supervisor: LivenessWindow must be positive")
	// ErrNegativeGrace is returned by New when GraceWindow < 0.
	ErrNegativeGrace = errors.New("supervisor: GraceWindow must be non-negative")
)

// Config carries the injected dependencies and the classification windows.
type Config struct {
	// Prober is the injected liveness proof (optional). Nil ⇒ heartbeat-age-only
	// classification. When non-nil it is AUTHORITATIVE in both directions whenever
	// it returns a non-UNKNOWN verdict (§11.4.6).
	Prober Prober
	// LivenessWindow is the maximum heartbeat age tolerated before the heartbeat is
	// STALE. Required, must be > 0.
	LivenessWindow time.Duration
	// GraceWindow is the additional band beyond LivenessWindow during which a
	// stale-heartbeat entity WITHOUT a death proof is only SUSPECT (not yet DEAD).
	// Must be >= 0. Zero ⇒ a stale heartbeat with no live proof is DEAD immediately
	// (no SUSPECT band).
	GraceWindow time.Duration
}

// Supervisor is the concurrency-safe watchdog. It owns the entity → last-seen map
// and the append-only event log. Every mutating decision is made under a single
// lock held only for the in-memory bookkeeping + the (fast, non-blocking) prober
// callback — never across file I/O.
type Supervisor struct {
	mu             sync.Mutex
	lastSeen       map[string]time.Time
	lastState      map[string]State
	events         []Event
	prober         Prober
	livenessWindow time.Duration
	graceWindow    time.Duration
}

// New returns a supervisor configured with the given windows and proof. It
// rejects a non-positive LivenessWindow (ErrNonPositiveWindow) and a negative
// GraceWindow (ErrNegativeGrace) — there is no fail-open default window.
func New(cfg Config) (*Supervisor, error) {
	if cfg.LivenessWindow <= 0 {
		return nil, ErrNonPositiveWindow
	}
	if cfg.GraceWindow < 0 {
		return nil, ErrNegativeGrace
	}
	return &Supervisor{
		lastSeen:       make(map[string]time.Time),
		lastState:      make(map[string]State),
		prober:         cfg.Prober,
		livenessWindow: cfg.LivenessWindow,
		graceWindow:    cfg.GraceWindow,
	}, nil
}

// Register begins monitoring entity id with an initial last-seen instant. A blank
// id is rejected (ErrEmptyEntity) and a repeated id is rejected (ErrDuplicate).
func (s *Supervisor) Register(id string, lastSeen time.Time) error {
	if id == "" {
		return ErrEmptyEntity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.lastSeen[id]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicate, id)
	}
	s.lastSeen[id] = lastSeen
	s.events = append(s.events, Event{At: lastSeen, Kind: EventRegister, Entity: id})
	return nil
}

// Heartbeat records that entity id was seen alive at instant `at` — the consumer-
// supplied liveness signal (the orchestrator/alias bumping its heartbeat each
// loop). It advances the stored last-seen ONLY forward: an out-of-order (older)
// heartbeat is ignored so a late-arriving stale beat can never make a live entity
// look older than it is (§11.4.6 — the freshest evidence wins). Returns
// ErrNotFound when the entity is not monitored.
func (s *Supervisor) Heartbeat(id string, at time.Time) error {
	if id == "" {
		return ErrEmptyEntity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok := s.lastSeen[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	if at.After(prev) {
		s.lastSeen[id] = at
	}
	return nil
}

// Remove deregisters entity id. Returns ErrNotFound when it is not monitored.
func (s *Supervisor) Remove(id string) error {
	if id == "" {
		return ErrEmptyEntity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.lastSeen[id]; !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	delete(s.lastSeen, id)
	delete(s.lastState, id)
	s.events = append(s.events, Event{At: s.zeroOrLast(), Kind: EventRemove, Entity: id})
	return nil
}

// zeroOrLast returns a best-effort timestamp for a REMOVE event; Remove has no
// caller-supplied instant, so it borrows the most recent event time (or zero).
func (s *Supervisor) zeroOrLast() time.Time {
	if n := len(s.events); n > 0 {
		return s.events[n-1].At
	}
	return time.Time{}
}

// Check classifies every monitored entity at instant `now` (the injected clock,
// §11.4.50 — passing the instant explicitly makes the pass fully deterministic and
// reproducible). It returns one Verdict per entity in stable, sorted-by-id order,
// and appends a TRANSITION event for every entity whose classification changed
// since the previous Check (the first observation is recorded as a transition
// from the empty state). It is the SIGNAL, not the action: the caller inspects the
// returned verdicts / DeadSet and drives recovery elsewhere (§11.4.6).
func (s *Supervisor) Check(now time.Time) []Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.sortedIDsLocked()
	out := make([]Verdict, 0, len(ids))
	for _, id := range ids {
		ls := s.lastSeen[id]
		proof := LivenessUnknown
		if s.prober != nil {
			proof = s.prober(id)
		}
		state, reason := Classify(ls, now, s.livenessWindow, s.graceWindow, proof)

		if prev, seen := s.lastState[id]; !seen || prev != state {
			from := prev // "" when unseen — the empty-state transition
			s.events = append(s.events, Event{
				At: now, Kind: EventTransition, Entity: id, From: from, To: state, Reason: reason,
			})
			s.lastState[id] = state
		}

		out = append(out, Verdict{
			Entity: id, State: state, Reason: reason, LastSeen: ls, Age: now.Sub(ls), Proof: proof,
		})
	}
	return out
}

// DeadSet is a READ-ONLY convenience: it classifies every entity at `now` and
// returns the ids currently classified DEAD, in sorted order — the set a caller
// acts on. It does NOT append events or mutate the remembered last-state (it is a
// peek, so repeated DeadSet calls never pollute the transition log that Check
// owns).
func (s *Supervisor) DeadSet(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dead []string
	for _, id := range s.sortedIDsLocked() {
		proof := LivenessUnknown
		if s.prober != nil {
			proof = s.prober(id)
		}
		if state, _ := Classify(s.lastSeen[id], now, s.livenessWindow, s.graceWindow, proof); state == StateDead {
			dead = append(dead, id)
		}
	}
	return dead
}

// Entities returns the sorted ids currently monitored.
func (s *Supervisor) Entities() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sortedIDsLocked()
}

// LastSeen returns the stored last-seen instant for id and true, or the zero time
// and false when id is not monitored.
func (s *Supervisor) LastSeen(id string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls, ok := s.lastSeen[id]
	return ls, ok
}

// Events returns an independent copy of the append-only audit spine. Mutating the
// returned slice never affects the supervisor.
func (s *Supervisor) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// sortedIDsLocked returns the monitored ids in sorted order. Callers MUST hold
// s.mu. Sorting makes every Check/DeadSet output order deterministic regardless of
// map-iteration order (§11.4.50).
func (s *Supervisor) sortedIDsLocked() []string {
	ids := make([]string, 0, len(s.lastSeen))
	for id := range s.lastSeen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
