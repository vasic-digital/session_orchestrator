# session_orchestrator

**Revision:** 1
**Last modified:** 2026-07-08T00:00:00Z
**License:** MIT
**Status:** early scaffold — first buildable increment (the alias-health registry + `is_operable` predicate). The same-session failover/resume spine is **NOT** implemented (its cross-config-dir `claude --resume` continuity premise is `UNCONFIRMED:` pending a POC).

A **project-agnostic, fully decoupled** Go engine for coordinating a *flowing
pool* of session aliases behind a *floating orchestrator role*, with a
no-fail-open health predicate that decides which alias may do work right now.

## The model (flowing pool + floating orchestrator role)

- **Flowing pool.** Every alias — first-party (native) and third-party
  (provider) — lives in one shared pool. No alias is reserved or locked for
  exclusive use.
- **Floating role.** The orchestrator is a *role* over that pool, not an alias
  removed from it. Track work and the orchestrator role draw from the same pool;
  the role is simply scheduled first, so it never starves.
- **Transient unpickability only.** An alias is unpickable only while it is
  *claimed* (single-owner) or *in cooldown*; it returns to the pickable set at
  its natural priority on release or cooldown expiry.

## What this increment ships

The `alias` package:

- **Registry** — a concurrency-safe set of aliases registered **by name only**
  (never credential values). Each alias carries its class (native/provider),
  capability rank, stable index, health state, and cooldown expiry.
- **`Classify`** — maps one live probe result onto the health taxonomy, scanning
  the body so an `HTTP 200` that hides a plan-cap string (e.g. a monthly usage
  limit) classifies as capped, never healthy.
- **`IsOperable`** — the total, **fail-closed** predicate: an alias is operable
  only on positive captured evidence (HTTP 200 + the `VERIFY_OK` token + no
  error body + a healthy classification + not in cooldown + not in a known-bad
  state). Every un-evaluable outcome yields `false`. There is no fail-open path.
- **`SortByPriority` / `FirstOperable`** — the deterministic priority order
  (native before provider, free before cooling, stronger before weaker, stable
  tie-break) and a first-operable selection that returns an explicit "none"
  rather than falling through to an unhealthy alias.

## Decoupling contract

This engine hardcodes **no** track, alias name, directory, threshold, or project
string. A consumer registers its own topology at runtime; the engine reads it. An
`Alias` holds **no** secret material — the caller performs the probe (passing any
key via env/config, never on the command line) and hands the engine only the
observable result.

## Build & test

```sh
go build ./...
go test -race -count=3 ./...
```

## Not yet implemented (honest boundary)

The same-session **failover/resume spine** (capture session id → detect limit →
quiesce to a safe boundary → atomic swap → `claude --resume` on another alias)
depends on an `UNCONFIRMED:` premise — cross-`CLAUDE_CONFIG_DIR` `claude --resume`
continuity — that a scratch-session POC must confirm before any implementation.
It is deliberately absent from this scaffold.
