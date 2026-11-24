# AI docs freshness stamp

Records the commit an AI (or agent-assisted human) last reconciled these docs at.
The docs-freshness CI reads it: a PR that bumps it is trusted and the AI is skipped
(economy path). See the "Docs freshness" rule in AGENTS.md / CLAUDE.md.

Before a PR: update stale docs, set verified_at_commit to your branch tip.
On arrival: if this is behind the code you touch, reconcile the docs FIRST.

verified_at_commit: 4f63d90a857695875eb405ae7d22bb3818b65493
verified_diff_sha256: b099b313bb3af240a309da58c80452c4fe379e416bcd3d30fe58d0b62b98bbc6
reconciler_schema: 1
verified_at: 2026-11-24
by: Codex
note: Reconciled planned RPC drain, bounded shutdown failures and GitHub race/broker CI. Static local checks only; executable validation belongs to CI.

exact_original_drain_source_review: Reviewed immutable first-Run completion handle and explicit pre-Run/repeated-Run refusal; all built-in admission stops before original joins, background cleanup includes exact afterRun even following partial startup. Waiter context never cancels owner; original Run budget and public Subscription/wire/queue/events preserved, custom legacy semantics bounded honestly. Actual HTTP and RabbitMQ Ack/connection-overrun, cancelled/concurrent waiters and startup regressions are CI-only. GreenSDK098 consumers and runtime pins remain unchanged.
