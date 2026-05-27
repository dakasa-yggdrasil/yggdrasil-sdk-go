# Changelog

All notable changes to `yggdrasil-sdk-go` are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.4.0] - 2026-05-26

Three additive packages so the three new public adapters
(`integration-efi`, `integration-nfeio`, `integration-stripe`) do not
each ship their own copy of the same primitives.

### Added

- **`sig/hmac`** — provider-specific webhook signature verifiers:
  - `VerifyStripe(payload, header, secret, toleranceSecs) (ts int64, err error)`
    — implements the `t=<unix>,v1=<hex>` scheme with a tolerance
    window. Supports multiple `v1=` components (key rotation).
  - `VerifyHubSignature256(payload, header, secret) error` — implements
    the `X-Hub-Signature-256: sha256=<hex>` scheme used by GitHub,
    NFe.io, EFI, and other webhook providers.
  - All verifiers use `crypto/subtle.ConstantTimeCompare`.
  - Typed errors: `ErrTimestampExpired`, `ErrSignatureMismatch`,
    `ErrMissingTimestamp`, `ErrMissingV1`, `ErrMalformedHeader`,
    `ErrMalformedHubSig`.
- **`mtls`** — loads `*tls.Config` from a PKCS#12 bundle:
  - `Load(cfg Config)` with `SourceFile`, `SourceBase64`, `SourceDisabled`.
  - `LoadFromEnv(prefix)` reading `{PREFIX}_MTLS_ENABLED`,
    `{PREFIX}_CERTIFICATE`, `{PREFIX}_CERTIFICATE_BASE64`,
    `{PREFIX}_CERTIFICATE_PASSWORD`.
  - Returns `(nil, nil)` when disabled so callers can construct a
    plain HTTP client without branching.
- **`webhookhttp`** — wraps `net/http.Server` with the framing every
  webhook receiver wants:
  - `Server`, `New(Config)`, `Handle(method, path, h, opts...)`,
    `ListenAndServe(ctx)`.
  - `HandlerOption`: `WithVerifyFunc`, `WithIdempotencyKey`,
    `WithMaxBodyBytes` (default 64KB).
  - Handler return → HTTP status mapping: `nil`→202, `ErrDuplicate`→200,
    `*TerminalError`→400, other error→500.
  - Helper primitives `ParseHMACSHA256Signature` and
    `VerifyHMACSHA256Header` re-export `sig/hmac.VerifyHubSignature256`
    for one-line verifier wiring.

### Dependencies

- Added `golang.org/x/crypto v0.52.0` (used by `mtls` for PKCS#12 decode).

### Migration notes

- Purely additive — no breaking changes to v0.3.x APIs.
- v0.3.x adapter binaries continue to build against v0.4.0 without
  modification.

## [v0.3.0] - 2026-05-26

### Added

- AMQP transport: connection-level reconnect watchdog
  (`rpc/amqp/amqp.go`) — re-dials the broker on disconnect so a
  RabbitMQ restart no longer wedges every adapter pod. See memory
  `project_amqp_cascade_root_cause_2026_05_26.md`.

## [v0.2.x and earlier]

See `git log` — pre-v0.3.0 history is small and undocumented.
