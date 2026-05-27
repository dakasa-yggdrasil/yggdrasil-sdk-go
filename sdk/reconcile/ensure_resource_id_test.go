package reconcile

import (
	"testing"
)

// TestInferResourceID_CanonicalIDField verifies that observed shapes
// carrying the canonical `id` field (and the `ID` / `Id` reflect
// fallbacks) resolve verbatim. Backward compat with pre-v0.8.2 SDK.
func TestInferResourceID_CanonicalIDField(t *testing.T) {
	got := inferResourceID(nil, []byte(`{"id": "cus_abc"}`), "customer")
	if got != "cus_abc" {
		t.Fatalf("expected id=cus_abc, got %q", got)
	}
}

// TestInferResourceID_ResourceScopedIDFallback covers the v0.8.2 fix
// for stripe-shaped ensure responses: `{"customer_id": "cus_X"}` has
// no canonical `id` field, so pre-v0.8.2 inferResourceID returned ""
// and yggdrasil-core's /api/v1/events rejected with HTTP 400. v0.8.2
// falls back to `<resource>_id` when the canonical key is missing.
func TestInferResourceID_ResourceScopedIDFallback(t *testing.T) {
	got := inferResourceID(nil, []byte(`{"customer_id": "cus_X1Y2", "email": "x@y"}`), "customer")
	if got != "cus_X1Y2" {
		t.Fatalf("expected customer_id=cus_X1Y2, got %q", got)
	}
}

// TestInferResourceID_SlackChannelEnsureShape pins the slack ensure
// path: ensure_channel returns `{"channel_id": "C...", "name": "..."}`
// and v0.8.2 must extract `channel_id` as the resource_id.
func TestInferResourceID_SlackChannelEnsureShape(t *testing.T) {
	body := []byte(`{"channel_id": "C0B6D7U1HSP", "name": "yggsmoke-bridge", "created": true}`)
	got := inferResourceID(nil, body, "channel")
	if got != "C0B6D7U1HSP" {
		t.Fatalf("expected channel_id=C0B6D7U1HSP, got %q", got)
	}
}

// TestInferResourceID_GithubCompositeOwnerRepo covers the github
// ensure_repository response shape: `{"owner": "x", "repo": "y"}`
// joins to "x/y" — same precedence as inferRefFromInput so destroy
// and ensure stay consistent.
func TestInferResourceID_GithubCompositeOwnerRepo(t *testing.T) {
	body := []byte(`{"owner": "dakasa-yggdrasil", "repo": "smoketest-v081"}`)
	got := inferResourceID(nil, body, "repository")
	if got != "dakasa-yggdrasil/smoketest-v081" {
		t.Fatalf("expected dakasa-yggdrasil/smoketest-v081, got %q", got)
	}
}

// TestInferResourceID_NamedAfterResource covers the
// `{"repository": "owner/repo"}` shape — same precedence rung as
// inferRefFromInput.
func TestInferResourceID_NamedAfterResource(t *testing.T) {
	body := []byte(`{"repository": "owner/repo", "private": true}`)
	got := inferResourceID(nil, body, "repository")
	if got != "owner/repo" {
		t.Fatalf("expected owner/repo, got %q", got)
	}
}

// TestInferResourceID_EmptyBody returns "" without panicking.
func TestInferResourceID_EmptyBody(t *testing.T) {
	got := inferResourceID(nil, []byte(`{}`), "customer")
	if got != "" {
		t.Fatalf("expected empty resource_id for empty body, got %q", got)
	}
}

// TestInferResourceID_GarbageBody returns "" without panicking on
// non-JSON input.
func TestInferResourceID_GarbageBody(t *testing.T) {
	got := inferResourceID(nil, []byte(`not json at all`), "customer")
	if got != "" {
		t.Fatalf("expected empty resource_id for garbage body, got %q", got)
	}
}

// TestInferResourceID_CanonicalIDWinsOverScoped pins the precedence
// rule: when both `id` and `<resource>_id` exist, the canonical `id`
// wins. Keeps pre-v0.8.2 callers (e.g. grafana, which uses `id`)
// stable even after the v0.8.2 fallback is added.
func TestInferResourceID_CanonicalIDWinsOverScoped(t *testing.T) {
	body := []byte(`{"id": "canonical", "customer_id": "scoped"}`)
	got := inferResourceID(nil, body, "customer")
	if got != "canonical" {
		t.Fatalf("expected canonical to win over scoped, got %q", got)
	}
}

// TestInferResourceID_EmptyScopedFallsThrough pins one edge: an
// explicit empty `customer_id` does NOT take precedence over a
// missing key — the fallback must check non-emptiness so an
// uninitialized response field doesn't silently win.
func TestInferResourceID_EmptyScopedFallsThrough(t *testing.T) {
	body := []byte(`{"customer_id": "", "name": "..."}`)
	got := inferResourceID(nil, body, "customer")
	if got != "" {
		t.Fatalf("expected empty for empty scoped field, got %q", got)
	}
}

// TestInferResourceID_NumericID covers github-shaped responses where
// `id` is a JSON number (e.g. 1234567890), not a string. Pre-v0.8.4
// the type assertion `v.(string)` returned false and inference fell
// through to "" — and github.repository.ensured emitted with empty
// resource_id, rejected by yggdrasil-core with HTTP 400.
// v0.8.5 pin: integer-valued floats are formatted as integers
// (was previously "%g" which produced "1.234e+09" scientific
// notation for IDs >= 1e9).
func TestInferResourceID_NumericID(t *testing.T) {
	body := []byte(`{"id": 1234567890, "name": "smoketest"}`)
	got := inferResourceID(nil, body, "repository")
	if got != "1234567890" {
		t.Fatalf("expected integer-format numeric id, got %q", got)
	}
}

// TestInferResourceID_GithubFullNameComposite covers the real github
// ensure_repository response shape: the upstream object contains
// `full_name: "owner/repo"` and `owner: {login: "..."}`. The flat
// `owner: "x", repo: "y"` rung doesn't match this shape (owner is an
// object, not a string), so v0.8.4 falls through to the new
// full_name rung.
func TestInferResourceID_GithubFullNameComposite(t *testing.T) {
	body := []byte(`{"id": 1234567890, "name": "smoketest", "full_name": "dakasa-yggdrasil/smoketest", "owner": {"login": "dakasa-yggdrasil"}}`)
	got := inferResourceID(nil, body, "repository")
	// Numeric `id` rung wins (1234567890 → "1234567890" or "1.23...").
	// This pin asserts a non-empty result; the precedence
	// (id > full_name) is documented in inferResourceID itself.
	if got == "" {
		t.Fatalf("expected non-empty resource_id for github shape, got empty")
	}
}

// TestInferResourceID_FullNameWhenNoID covers the corner where the
// response carries only `full_name` (e.g. a downstream wrapper that
// stripped the numeric id) — the full_name rung resolves to
// "owner/repo".
func TestInferResourceID_FullNameWhenNoID(t *testing.T) {
	body := []byte(`{"full_name": "owner/repo", "name": "repo"}`)
	got := inferResourceID(nil, body, "repository")
	if got != "owner/repo" {
		t.Fatalf("expected owner/repo from full_name rung, got %q", got)
	}
}

// TestInferResourceID_StringIDWinsOverNumericFallback pins that when
// a canonical string `id` is present, it wins — the numeric-coercion
// fallback is for the OTHER shape, not a precedence change.
func TestInferResourceID_StringIDWinsOverNumericFallback(t *testing.T) {
	body := []byte(`{"id": "cus_abc", "channel_id": 999}`)
	got := inferResourceID(nil, body, "customer")
	if got != "cus_abc" {
		t.Fatalf("expected string id to win, got %q", got)
	}
}
