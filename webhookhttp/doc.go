// Package webhookhttp wraps net/http.Server with the request-handling
// pattern shared by every Yggdrasil adapter that receives inbound
// webhook deliveries (integration-stripe, integration-efi,
// integration-nfeio, …).
//
// The pattern: read the raw request body once (so HMAC checks cover
// the exact bytes the provider signed), apply a per-route verifier
// (HMAC, mTLS, custom), extract an idempotency key, and dispatch to
// a Handler that returns nil/ErrDuplicate/TerminalError/other-err for
// 202 / 200 / 400 / 500 respectively.
//
// The package owns the framing, body-size limit, and graceful
// shutdown. It does not own the dedup store (each adapter picks its
// own — sync.Map for stripe, LRU for nfe.io, DB constraint for efi).
package webhookhttp
