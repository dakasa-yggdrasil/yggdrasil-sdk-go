# AI docs freshness stamp

Records the commit an AI (or agent-assisted human) last reconciled these docs at.
The docs-freshness CI reads it: a PR that bumps it is trusted and the AI is skipped
(economy path). See the "Docs freshness" rule in AGENTS.md / CLAUDE.md.

Before a PR: update stale docs, set verified_at_commit to your branch tip.
On arrival: if this is behind the code you touch, reconcile the docs FIRST.

verified_at_commit: 47b12f509272436968509790bbdca8e927d47fe5
verified_at: 2026-11-24
by: Codex
note: Reconciled planned RPC drain, bounded shutdown failures and GitHub race/broker CI. Static local checks only; executable validation belongs to CI.
