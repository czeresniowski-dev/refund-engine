# refund-engine

A small Rust library that models refunds as an explicit, auditable state
machine against a real Postgres, so a reader can run the code and watch the
claims from the article hold. It exists because every payments system I worked
on started with refunds as an afterthought (a button that called
`gateway.refund(charge_id)` and trusted the return value) and every one
eventually paid for that, sometimes literally. The dangerous part of a refund
is not the happy path; it is the recovery from the refund that half-happened
and the record that proves which half. This repo backs the article
[Why refund processing is harder than it looks](https://czeresniowski.dev/writing/why-refund-processing-is-harder-than-it-looks).

The app snippets in the article are Python. This is a faithful Rust port: same
SQL, same identifiers, same control flow. Database access uses runtime sqlx
(`sqlx::query`, `sqlx::query_as`, `#[derive(sqlx::FromRow)]`), never the
compile-time `query!` macros, so the crate compiles without a database. SQL
migrations under `migrations/` are embedded with `sqlx::migrate!()` and applied
at runtime.

## Quickstart

```sh
docker compose up -d
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/refund_engine

# the headline: seed ~200 refunds, SIGKILL the batch mid-flight, recover
cargo run --example demo_incident
```

Expected tail of the headline demo:

```
seeded 200 requested refunds
batch died: 120 rows got a recorded gateway_ref, 60 rows submitted-with-dropped-response, 20 rows never attempted; gateway has issued 180 refunds so far
recovery walked 200 stuck rows: 180 already at gateway (left for settle webhook), 20 resubmitted with the same key
gateway issued 180 -> 200 refunds across recovery
0 double-refunds across 200 recovered (200 rows converged, 0 unconverged)
OK: 0 double-refunds across 200 recovered; every row converged
```

Run the rest, and the test suite (DB tests are serial):

```sh
cargo run --example demo_over_refund
cargo run --example demo_idempotency
cargo run --example demo_webhook
cargo run --example demo_separation
cargo run --example demo_reconcile

cargo test -- --test-threads=1
```

The examples and tests reset their own tables at the start, so they pass on
re-run. The database at `DATABASE_URL` is fully owned by this repo; it gets
truncated freely.

## What each demo proves

Each example reproduces one section of
[the article](https://czeresniowski.dev/writing/why-refund-processing-is-harder-than-it-looks).

| Example | Article section | Claim it backs |
| --- | --- | --- |
| `demo_over_refund` | Preventing over-refund under concurrency | Two concurrent 60.00 refunds on a 100.00 capture: exactly one succeeds, the other is rejected by the invariant. A naive check-then-insert would allow 120.00. |
| `demo_idempotency` | Idempotency keys done right | Four submit calls with the same key (the refund's own uuid) make the gateway issue exactly one refund; the row stays `submitted`, never `settled` on optimism. |
| `demo_webhook` | Hostile webhooks | Out-of-order and duplicated webhooks are tolerated; bad signatures are rejected; a late `settled` never resurrects a `failed` refund. |
| `demo_incident` | The 12k incident | A SIGKILLed batch leaves rows stuck; recovery queries the gateway by idempotency key and converges with `0 double-refunds across 200 recovered`. |
| `demo_separation` | Audit, approval, and PII | A large refund enters `pending_review`; self-approval is rejected (`approver != requested_by`); a third party approves; a PAN is redacted to last four. |
| `demo_reconcile` | Reconciliation is co-owned | A settlement file with one of each discrepancy class is flagged into exactly the three classes; a clean match is flagged by none. |

## How it maps to the article

- `migrations/0001_refunds.sql` — the schema. `refunds` is the article's
  `CREATE TABLE` verbatim, including the `CHECK (status IN (...))`, the
  `idempotency_key text NOT NULL UNIQUE`, the `refunds_charge_id_idx`, and the
  partial unique index on `gateway_ref`. `refund_transitions` is the append-only
  audit table; `webhook_events` carries the event-id dedupe; `charges` is the
  reduced charge a refund points at.
- `src/model.rs` — `Refund` and the `Status` enum mirroring the CHECK; charge
  helpers and the `sum(refunds)` helper that excludes `failed`/`canceled`.
- `src/request.rs` — "Preventing over-refund under concurrency". The
  `BEGIN; SELECT amount_captured, currency FROM charges WHERE id=$1 FOR UPDATE;`
  then `SELECT COALESCE(SUM(amount_cents),0) ...`, the
  `new_amount <= captured - refunded` and currency assertions, the refund
  `INSERT` (with `idempotency_key` = the refund's own uuid), and the
  `refund_transitions` insert, all in one transaction. Amounts at or above the
  review threshold start in `pending_review`.
- `src/submit.rs` — "Idempotency keys done right". The article's `submit_refund`
  ported: skip terminal rows; UPDATE to `submitted` guarded by
  `status IN ('requested','submitted')`; call the gateway with
  `idempotency_key = refund_id`; record `gateway_ref` but never set `settled`.
- `src/webhook.rs` — "Hostile webhooks". HMAC signature verification,
  `INSERT INTO webhook_events ... ON CONFLICT DO NOTHING RETURNING id` dedupe,
  the order-tolerant `settled` update guarded by
  `status IN ('submitted','requested')`, and the `failed` update guarded by
  `status NOT IN ('settled','canceled')`. Each transition is written with actor
  `webhook` in the same transaction as the state change.
- `src/approval.rs` — "Audit, approval, and PII". `approve` moves
  `pending_review -> requested`, enforcing `approver != requested_by`, and
  records the approver in the audit table.
- `src/gateway.rs` — the `Gateway` trait and `MockGateway` that dedupes on the
  idempotency key (a second create with the same key returns the original
  without issuing a second refund). It can be configured to accept but drop the
  response (the crash window) or to time out. This mock is what makes the
  incident demo real.
- `src/recover.rs` — "The 12k incident". `recover_submitted` is the
  converge-don't-guess procedure: for every stuck row, ask the gateway by
  idempotency key; if it exists, record the `gateway_ref` and wait for the
  settle webhook; if not, resubmit safely with the same key.
- `src/reconcile.rs` — "Reconciliation is co-owned". `reconcile` matches
  `settled` refunds against a settlement file by `gateway_ref` and amount,
  flagging the three discrepancy classes.
- `src/redact.rs` — "Audit, approval, and PII". `redact_pan` drops anything of a
  card-number shape and keeps the last four before it reaches a log.

## Where this breaks / when not to use it

The article is honest about its own limits, and so is this repo.

- The gateway here is an in-crate mock, not a network client. It gives us the
  one property the whole design leans on (dedupe on the idempotency key, accept
  but drop the response, time out), which is enough to reproduce the incident,
  but it is not Stripe. In production the gateway's idempotency is one layer of
  three, alongside your durable record of intent and the bank's settlement file.
- This is the deliberately boring middle path: a mutable `status` column plus an
  append-only audit table. If money has to balance to the cent across many
  accounts, build a double-entry ledger instead. If you need general long-lived
  orchestration with many steps, a durable-execution engine (Temporal, Step
  Functions) may fit better; the trade this repo makes is debuggability (any
  on-call engineer, finance analyst, or support agent can answer "what state is
  this refund in" with a plain `SELECT`).
- The over-refund invariant is enforced with a `SELECT ... FOR UPDATE` row lock
  scoped to one charge, not `SERIALIZABLE` isolation. On a very hot charge the
  lock serializes attempts; that is the intended cost. `SERIALIZABLE` also works
  but trades the lock for serialization-failure retries.
- The system that can issue 200 refunds in this demo (or 12,000 in the
  incident) needs a brake. A rate limiter and circuit breaker on bulk
  auto-refund flows are described in the article as the guardrail that should
  have existed; they are out of scope for this library.

## Sibling repos

Part of a set of small repos that each back one article:

- [skip-locked](https://github.com/czeresniowski-dev/skip-locked) — work queues with `FOR UPDATE SKIP LOCKED`.
- [pg-outbox](https://github.com/czeresniowski-dev/pg-outbox) — the transactional outbox; how the settle/fail webhooks would be published reliably.
- [idem-key](https://github.com/czeresniowski-dev/idem-key) — idempotency keys as a reusable primitive, the generalization of this repo's `idempotency_key` discipline.
- [refund-engine](https://github.com/czeresniowski-dev/refund-engine) — this repo.
