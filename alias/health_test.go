package alias

import (
	"errors"
	"testing"
	"time"
)

// t0 is a fixed reference clock so every test is deterministic (§11.4.50).
var t0 = time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

// errTimeout stands in for a first-byte / transport timeout.
var errTimeout = errors.New("probe: context deadline exceeded")

// TestClassify covers the probe→state mapping, including the load-bearing
// HTTP-200-with-error-body scan that defeats the status-code-only bluff.
func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		pr   ProbeResult
		want State
	}{
		// Golden-good — the only path to HEALTHY.
		{"200_verify_ok", ProbeResult{HTTPStatus: 200, Body: VerifyToken}, StateHealthy},
		{"200_verify_ok_wrapped", ProbeResult{HTTPStatus: 200, Body: `{"text":"VERIFY_OK"}`}, StateHealthy},

		// Golden-bad — HTTP 200 bodies that hide an error (never HEALTHY).
		{"200_kimi_monthly_cap", ProbeResult{HTTPStatus: 200, Body: "You've reached kimi monthly usage limit"}, StateProviderCap},
		{"200_quota_exceeded", ProbeResult{HTTPStatus: 200, Body: "Your quota has been exceeded"}, StateProviderCap},
		{"200_weekly_limit", ProbeResult{HTTPStatus: 200, Body: "weekly usage limit reached"}, StateWeeklyLimit},
		{"200_session_limit", ProbeResult{HTTPStatus: 200, Body: "session limit reached, try later"}, StateSessionLimit},
		{"200_rate_limited_body", ProbeResult{HTTPStatus: 200, Body: "error: rate-limited"}, StateSustained429},
		{"200_overloaded_body", ProbeResult{HTTPStatus: 200, Body: "upstream Overloaded"}, StateApiOverloaded},
		{"200_unauthorized_body", ProbeResult{HTTPStatus: 200, Body: "invalid api key"}, StateAuthDead},
		{"200_empty_no_token", ProbeResult{HTTPStatus: 200, Body: ""}, StateUnknown},
		{"200_no_token", ProbeResult{HTTPStatus: 200, Body: "hello world"}, StateUnknown},

		// Transport-status classifications.
		{"401", ProbeResult{HTTPStatus: 401, Body: "Unauthorized"}, StateAuthDead},
		{"403", ProbeResult{HTTPStatus: 403, Body: "Forbidden"}, StateAuthDead},
		{"429", ProbeResult{HTTPStatus: 429, Body: "Too Many Requests"}, StateSustained429},
		{"529", ProbeResult{HTTPStatus: 529, Body: "Overloaded"}, StateApiOverloaded},
		{"500", ProbeResult{HTTPStatus: 500, Body: "boom"}, StateSustained5xx},
		{"503", ProbeResult{HTTPStatus: 503, Body: "unavailable"}, StateSustained5xx},
		{"302_other", ProbeResult{HTTPStatus: 302, Body: ""}, StateUnknown},
		{"transport_error", ProbeResult{Err: errTimeout}, StateUnreachable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.pr); got != c.want {
				t.Fatalf("Classify(%q) = %q; want %q", c.name, got, c.want)
			}
		})
	}
}

// TestIsOperable is the anti-bluff heart: the no-fail-open predicate. Only the
// golden-good probe on a clean alias is operable; every golden-bad probe and
// every persisted-bad-state / cooldown case is NOT operable.
func TestIsOperable(t *testing.T) {
	clean := Alias{Name: "a", Class: ClassNative, State: StateHealthy}

	cases := []struct {
		name  string
		alias Alias
		pr    ProbeResult
		now   time.Time
		want  bool
	}{
		// Golden-good: healthy probe, clean alias, no cooldown → operable.
		{"healthy", clean, ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, true},
		{"approaching_limit_still_pickable", Alias{Name: "a", State: StateApproachingLimit},
			ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, true},

		// Golden-bad probes (WS-F §2.2) — none operable despite any status.
		{"kimi_cap_200", clean, ProbeResult{HTTPStatus: 200, Body: "You've reached kimi monthly usage limit"}, t0, false},
		{"http_429", clean, ProbeResult{HTTPStatus: 429, Body: "Too Many Requests"}, t0, false},
		{"http_401", clean, ProbeResult{HTTPStatus: 401, Body: "Unauthorized"}, t0, false},
		{"first_byte_timeout", clean, ProbeResult{Err: errTimeout}, t0, false},
		{"200_no_verify_token", clean, ProbeResult{HTTPStatus: 200, Body: "ok"}, t0, false},
		{"empty_body", clean, ProbeResult{HTTPStatus: 200, Body: ""}, t0, false},

		// Persisted-state gate: healthy probe cannot override a known-bad state.
		{"auth_dead_persisted", Alias{Name: "a", State: StateAuthDead},
			ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, false},
		{"provider_cap_persisted", Alias{Name: "a", State: StateProviderCap},
			ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, false},

		// Cooldown gate: in-cooldown → not operable; after expiry → operable.
		{"in_cooldown", Alias{Name: "a", State: StateHealthy, ExhaustedUntil: t0.Add(time.Minute)},
			ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, false},
		{"cooldown_elapsed", Alias{Name: "a", State: StateHealthy, ExhaustedUntil: t0.Add(-time.Minute)},
			ProbeResult{HTTPStatus: 200, Body: VerifyToken}, t0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsOperable(c.alias, c.pr, c.now); got != c.want {
				t.Fatalf("IsOperable(%q) = %v; want %v", c.name, got, c.want)
			}
		})
	}
}

// TestAnalyzerSelfValidation is the §11.4.107(10) golden-good/golden-bad
// self-validation: a comprehensive golden-bad set MUST all classify away from
// HEALTHY and MUST all be non-operable. If a future edit made the analyzer trust
// the status code alone (the paired-mutation target), the kimi-cap 200 fixture
// would classify HEALTHY and this test would FAIL — proving the body scan is
// load-bearing (WS-F §2.2 meta-test).
func TestAnalyzerSelfValidation(t *testing.T) {
	goldenGood := ProbeResult{HTTPStatus: 200, Body: VerifyToken}
	if Classify(goldenGood) != StateHealthy {
		t.Fatalf("golden-good must classify HEALTHY, got %q", Classify(goldenGood))
	}
	if !IsOperable(Alias{Name: "g", State: StateHealthy}, goldenGood, t0) {
		t.Fatal("golden-good must be operable")
	}

	goldenBad := []ProbeResult{
		{HTTPStatus: 200, Body: "You've reached kimi monthly usage limit"},
		{HTTPStatus: 200, Body: "insufficient credit"},
		{HTTPStatus: 429, Body: "Too Many Requests"},
		{HTTPStatus: 401, Body: "Unauthorized"},
		{HTTPStatus: 500, Body: "internal error"},
		{Err: errTimeout},
		{HTTPStatus: 200, Body: ""},
	}
	for i, bad := range goldenBad {
		if Classify(bad) == StateHealthy {
			t.Fatalf("golden-bad[%d] classified HEALTHY (fail-open bluff): %+v", i, bad)
		}
		if IsOperable(Alias{Name: "b", State: StateHealthy}, bad, t0) {
			t.Fatalf("golden-bad[%d] reported operable (fail-open bluff): %+v", i, bad)
		}
	}
}

// TestClassifyErrorBody exercises the exported analyzer directly.
func TestClassifyErrorBody(t *testing.T) {
	if _, matched := ClassifyErrorBody(VerifyToken); matched {
		t.Fatal("VERIFY_OK body must not match any error pattern")
	}
	if st, matched := ClassifyErrorBody("You've reached kimi monthly usage limit"); !matched || st != StateProviderCap {
		t.Fatalf("kimi cap: got (%q,%v); want (PROVIDER_CAP,true)", st, matched)
	}
}
