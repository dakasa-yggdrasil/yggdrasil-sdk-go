// Package hmac provides HMAC-SHA256 webhook signature verifiers for
// the wire formats used by external payment / event providers
// consumed by Yggdrasil integration adapters.
//
// The package intentionally exposes one function per provider
// signature shape rather than a generic "Verify" — different
// providers encode timestamps, key prefixes, and tolerance windows
// differently, and a single typed entry point keeps callers honest
// about which scheme they signed up for.
//
// Currently supported:
//
//   - Stripe (Stripe-Signature header, t=<unix>,v1=<hex>, with
//     tolerance window) — VerifyStripe.
//   - GitHub / EFI / NFe.io style HubSignature256 (X-Hub-Signature-256
//     header, sha256=<hex>) — VerifyHubSignature256.
//
// All verifiers use constant-time comparison (crypto/hmac.Equal) and
// never log the secret or the computed/expected HMAC bytes.
package hmac
