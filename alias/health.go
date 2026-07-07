// Package alias implements the alias-health layer of the session_orchestrator
// engine: a project-agnostic registry of session aliases plus the no-fail-open
// is_operable predicate that decides, from a single live probe result, whether
// an alias may currently be selected for work.
//
// Decoupling contract (§11.4.28 / §11.4.177): this package hardcodes NO track,
// alias name, directory, threshold, or project string. A consumer registers its
// own aliases (by NAME only — never credential values, §11.4.10) at runtime; the
// engine reads them. An Alias carries no secret material.
//
// Anti-bluff contract (§11.4.69): "present in the registry" is never "operable
// now". IsOperable returns true ONLY on positive captured evidence (HTTP 200 +
// the VERIFY_OK token + no error-body + a healthy classification + not in
// cooldown + not in a known-bad state). Any un-evaluable outcome — network
// error, timeout, empty body, HTTP-200-with-error-body, 4xx/5xx — yields false.
// There is NO path that converts absence of evidence into "healthy".
package alias

import (
	"regexp"
	"strings"
	"time"
)

// VerifyToken is the exact positive-evidence marker a healthy probe body must
// contain. It mirrors the toolkit probe prompt "Reply with exactly: VERIFY_OK"
// (WS-B §4.2). A 200 response lacking this token is NOT positive evidence.
const VerifyToken = "VERIFY_OK"

// Class distinguishes the two alias families of the flowing pool (WS-B §3).
// Native aliases (subscription / OAuth) are always preferred first; provider
// aliases are the fallback tier.
type Class int

const (
	// ClassNative is a first-party subscription/OAuth alias (rank 0, preferred).
	ClassNative Class = iota
	// ClassProvider is a third-party API-key alias (rank 1, fallback).
	ClassProvider
)

// State is the health taxonomy an alias can hold. The healthy value D0 plus the
// documented degraded/exhausted values (WS-A taxonomy, WS-B §4.4) are modelled;
// StateUnknown is the honest verdict for an ambiguous response that is neither a
// confirmed-healthy nor a recognised failure (never treated as operable).
type State string

const (
	StateHealthy          State = "HEALTHY"           // D0 — positive evidence
	StateApproachingLimit State = "APPROACHING_LIMIT" // proactive headroom low; still pickable
	StateAuthDead         State = "AUTH_DEAD"         // D8 — 401/403 / auth-error body
	StateSessionLimit     State = "SESSION_LIMIT"     // per-session cap reached
	StateWeeklyLimit      State = "WEEKLY_LIMIT"      // weekly cap reached
	StateSustained429     State = "SUSTAINED_429"     // sustained rate-limit
	StateSustained5xx     State = "SUSTAINED_5XX"     // sustained server error
	StateApiOverloaded    State = "API_OVERLOADED"    // D7 — 529 / overloaded body
	StateProviderCap      State = "PROVIDER_CAP"      // D10 — plan / usage / quota cap body
	StateUnreachable      State = "UNREACHABLE"       // network down / timeout / tool missing
	StateUnknown          State = "UNKNOWN"           // ambiguous — cannot confirm healthy
)

// excludedStates is the persisted-state exclusion set from the is_operable
// predicate (WS-B §4.4): an alias whose stored State is any of these is never
// operable, regardless of a momentarily-healthy probe, until a recovery flow or
// an operator action clears it.
var excludedStates = map[State]bool{
	StateAuthDead:      true,
	StateSessionLimit:  true,
	StateWeeklyLimit:   true,
	StateSustained429:  true,
	StateSustained5xx:  true,
	StateApiOverloaded: true,
	StateProviderCap:   true,
	StateUnreachable:   true,
}

// ProbeResult is the outcome of one live health probe against an alias endpoint.
// It carries NO credential — the caller performs the request (passing the key
// via env/config per §11.4.10) and hands the engine only the observable result.
type ProbeResult struct {
	HTTPStatus int    // transport status; 0 when the request never completed
	Body       string // response body (scanned for VERIFY_OK and error patterns)
	Err        error  // non-nil for a transport/timeout/tool-missing failure
}

// errRule maps an error-body regexp to the health State it implies. Order is
// significant: more specific rules precede broader ones so the most accurate
// classification wins. All patterns are case-insensitive.
type errRule struct {
	re    *regexp.Regexp
	state State
}

var errRules = []errRule{
	{regexp.MustCompile(`(?i)weekly\s+(usage\s+)?limit`), StateWeeklyLimit},
	{regexp.MustCompile(`(?i)session\s+limit`), StateSessionLimit},
	{regexp.MustCompile(`(?i)monthly\s+(usage\s+)?limit`), StateProviderCap},
	{regexp.MustCompile(`(?i)usage\s+limit`), StateProviderCap},
	{regexp.MustCompile(`(?i)quota`), StateProviderCap},
	{regexp.MustCompile(`(?i)insufficient\s+(balance|credit|funds)`), StateProviderCap},
	{regexp.MustCompile(`(?i)(billing|payment\s+required)`), StateProviderCap},
	{regexp.MustCompile(`(?i)model\s+.*(not\s+available|not\s+found|unavailable)`), StateProviderCap},
	{regexp.MustCompile(`(?i)(rate.?limit|too\s+many\s+requests)`), StateSustained429},
	{regexp.MustCompile(`(?i)overloaded`), StateApiOverloaded},
	{regexp.MustCompile(`(?i)(unauthori[sz]|invalid\s+api\s+key|authentication\s+fail|access\s+denied|permission\s+denied|forbidden)`), StateAuthDead},
}

// ClassifyErrorBody scans a response body for a known error pattern. It returns
// the implied State and true on the first match, or ("", false) when the body
// carries no recognised error signal. Exported so a self-validation harness can
// exercise the analyzer directly (§11.4.107(10)).
func ClassifyErrorBody(body string) (State, bool) {
	for _, r := range errRules {
		if r.re.MatchString(body) {
			return r.state, true
		}
	}
	return "", false
}

// Classify maps one ProbeResult onto the health taxonomy. It is the single
// source of a probe's verdict and the body-scan that defeats the
// HTTP-200-with-error-body bluff (WS-B §4.2): a 200 whose body hides a plan-cap
// string classifies as PROVIDER_CAP, never HEALTHY.
func Classify(pr ProbeResult) State {
	if pr.Err != nil {
		return StateUnreachable
	}
	switch {
	case pr.HTTPStatus == 401, pr.HTTPStatus == 403:
		return StateAuthDead
	case pr.HTTPStatus == 429:
		return StateSustained429
	case pr.HTTPStatus == 529:
		return StateApiOverloaded
	case pr.HTTPStatus >= 500:
		return StateSustained5xx
	case pr.HTTPStatus != 200:
		return StateUnknown
	}
	// HTTP 200 — still scan the body before trusting the status code.
	if st, matched := ClassifyErrorBody(pr.Body); matched {
		return st
	}
	if !strings.Contains(pr.Body, VerifyToken) {
		return StateUnknown // 200 without the positive-evidence token
	}
	return StateHealthy
}

// IsOperable is the total, fail-closed predicate deciding whether an alias may
// be selected right now (WS-B §4.4). It returns true ONLY when the live probe
// classifies HEALTHY AND the alias is not in cooldown AND its persisted State is
// not in the excluded set. Every other outcome is false — there is no fail-open.
func IsOperable(a Alias, pr ProbeResult, now time.Time) bool {
	if Classify(pr) != StateHealthy {
		return false
	}
	if now.Before(a.ExhaustedUntil) {
		return false // cooldown has not elapsed
	}
	if excludedStates[a.State] {
		return false // known-bad persisted condition
	}
	return true
}
