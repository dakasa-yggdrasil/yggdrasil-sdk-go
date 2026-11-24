# AI docs freshness stamp

Records the commit an AI (or agent-assisted human) last reconciled these docs at.
The docs-freshness CI reads it: a PR that bumps it is trusted and the AI is skipped
(economy path). See the "Docs freshness" rule in AGENTS.md / CLAUDE.md.

Before a PR: update stale docs, set verified_at_commit to your branch tip.
On arrival: if this is behind the code you touch, reconcile the docs FIRST.

verified_at_commit: 7be45d967ef903b32eef8e814ec3d1a918c9c86e
verified_diff_sha256: 10e76f5101aff894c16b2eb155ca989aaa45fc913a484824b169ec39ec963293
reconciler_schema: 1
verified_at: 2026-11-24
by: Codex
note: Reconciled planned RPC drain, bounded shutdown failures and GitHub race/broker CI. Static local checks only; executable validation belongs to CI.

exact_original_drain_source_review: Reviewed immutable first-Run completion handle and explicit pre-Run/repeated-Run refusal; all built-in admission stops before original joins, background cleanup includes exact afterRun even following partial startup. Waiter context never cancels owner; original Run budget and public Subscription/wire/queue/events preserved, custom legacy semantics bounded honestly. Actual HTTP and RabbitMQ Ack/connection-overrun, cancelled/concurrent waiters and startup regressions are CI-only. GreenSDK098 consumers and runtime pins remain unchanged.

exact_drain_http_fixture_envelope_review: Original genuine CI rejected only a plaintext substring assertion against the existing base64 Body RPC envelope. Decode rpc.Envelope and require exact original Body plus content type; production implementation/wire contract unchanged. Preserve original failed head/CI; successor validation is CI-only.
