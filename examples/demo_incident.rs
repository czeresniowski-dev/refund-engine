//! The 12k incident (headline).
//!
//! Seed ~200 refunds and run a batch submit that is SIGKILL-simulated partway
//! through: the gateway starts DROPPING responses, so the batch leaves some
//! rows `requested`, many `submitted` with the gateway having actually accepted
//! the refund, none `settled`, and the response never recorded. Then run
//! `recover_submitted`, which asks the gateway BY IDEMPOTENCY KEY whether each
//! stuck refund already happened and converges without guessing.
//!
//! The assertion that matters: the gateway issued each refund AT MOST ONCE, so
//! the double-refund count is zero, and every row converges.

use refund_engine::{
    connect_and_migrate, model, recover, request, reset, submit, MockConfig, MockGateway,
};

const N: usize = 200;
const KILL_AT: usize = 120; // responses start getting dropped here

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&url).await?;
    reset(&pool).await?;

    let gateway = MockGateway::new();

    // Seed ~200 requested refunds, each against its own charge.
    let mut refund_ids = Vec::with_capacity(N);
    for _ in 0..N {
        let charge = model::create_charge(&pool, 50_00, "usd").await?;
        let id = request::request_refund(&pool, charge.id, 20_00, "usd", "job:carrier-policy", Some("carrier_guarantee")).await?;
        refund_ids.push(id);
    }
    println!("seeded {N} requested refunds");

    // Batch submit. Partway through, an unrelated deploy SIGKILLs the worker:
    // we model that as the gateway accepting the refund but the response never
    // making it back (drop_response). Some rows never get called at all.
    // Rows [0, KILL_AT)        -> submitted, gateway_ref recorded (clean).
    // Rows [KILL_AT, NEVER_AT)  -> submitted, gateway accepted, response DROPPED.
    // Rows [NEVER_AT, N)        -> worker dead; never attempted, left 'requested'.
    const NEVER_AT: usize = KILL_AT + 60;
    let mut clean = 0;
    let mut dropped = 0;
    let mut never_attempted = 0;
    for (i, id) in refund_ids.iter().enumerate() {
        if i == KILL_AT {
            // The SIGKILL window opens: gateway accepts but drops every response.
            gateway.set_config(MockConfig { drop_response: true, ..Default::default() });
        }
        if i >= NEVER_AT {
            // Worker is fully dead; these rows stay 'requested', never submitted.
            never_attempted += 1;
            continue;
        }
        let accepted = submit::submit_refund(&pool, *id, &gateway).await?;
        if accepted {
            clean += 1;
        } else {
            dropped += 1;
        }
    }

    let issued_before = gateway.issued_count();
    println!(
        "batch died: {clean} rows got a recorded gateway_ref, \
         {dropped} rows submitted-with-dropped-response, {never_attempted} rows never attempted; \
         gateway has issued {issued_before} refunds so far"
    );

    // Sanity: nothing is settled, and the gateway has issued at most N.
    let settled_before: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM refunds WHERE status='settled'")
            .fetch_one(&pool)
            .await?;
    assert_eq!(settled_before.0, 0, "nothing should be settled yet");

    // --- Recovery: converge, don't guess. ---
    // The worker comes back and we run the runbook against a healthy gateway:
    // stop dropping responses so recovery can record refs and resubmit cleanly.
    gateway.set_config(MockConfig::default());
    let report = recover::recover_submitted(&pool, &gateway).await?;
    let issued_after = gateway.issued_count();

    println!(
        "recovery walked {} stuck rows: {} already at gateway (left for settle webhook), {} resubmitted with the same key",
        report.walked, report.already_at_gateway, report.resubmitted
    );
    println!("gateway issued {issued_before} -> {issued_after} refunds across recovery");

    // The headline invariant: the gateway issued each logical refund at most
    // once. With N distinct keys, issued_count can never exceed N, and a
    // double-refund would show up as issued > number of distinct refunds.
    let distinct_keys: (i64,) =
        sqlx::query_as("SELECT COUNT(DISTINCT idempotency_key) FROM refunds")
            .fetch_one(&pool)
            .await?;
    let double_refunds = (issued_after as i64) - distinct_keys.0.min(issued_after as i64);

    // Every row converged: no row is left without a gateway_ref, and every row
    // is either submitted (awaiting settle webhook) or already terminal.
    let unconverged: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM refunds WHERE gateway_ref IS NULL OR status NOT IN ('submitted','settled','failed','canceled')",
    )
    .fetch_one(&pool)
    .await?;

    println!(
        "0 double-refunds across {} recovered ({} rows converged, {} unconverged)",
        report.walked,
        N as i64 - unconverged.0,
        unconverged.0
    );

    assert!(issued_after as i64 <= N as i64, "gateway must never issue more than N refunds");
    assert_eq!(double_refunds, 0, "zero double-refunds");
    assert_eq!(unconverged.0, 0, "every row must converge to a recorded gateway_ref and a known state");

    println!("OK: 0 double-refunds across {} recovered; every row converged", report.walked);
    Ok(())
}
