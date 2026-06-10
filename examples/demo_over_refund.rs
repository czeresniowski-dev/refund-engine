//! Preventing over-refund under concurrency.
//!
//! A charge captured at 100.00 dollars. Fire two concurrent 60.00 partial
//! refunds (a support agent and an automated returns job). The `FOR UPDATE`
//! lock on the charge row serializes them, so exactly one succeeds and the
//! other is rejected by the invariant. A naive check-then-insert would let both
//! through and refund 120.00 against a 100.00 capture.

// Money literals read as dollars_cents, e.g. `100_00` = 100 dollars 00 cents.
#![allow(clippy::inconsistent_digit_grouping)]

use std::sync::Arc;

use refund_engine::error::RefundError;
use refund_engine::{connect_and_migrate, model, request, reset};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = Arc::new(connect_and_migrate(&url).await?);
    reset(&pool).await?;

    // Charge captured at 100.00 dollars = 100_00 cents.
    let charge = model::create_charge(&pool, 100_00, "usd").await?;
    println!("charge {} captured at {} cents", charge.id, charge.amount_captured);

    // Two concurrent 60.00 refunds against the same charge.
    let p1 = pool.clone();
    let p2 = pool.clone();
    let cid = charge.id;

    let t1 = tokio::spawn(async move {
        request::request_refund(&p1, cid, 60_00, "usd", "user:support-agent", Some("goodwill"))
            .await
    });
    let t2 = tokio::spawn(async move {
        request::request_refund(&p2, cid, 60_00, "usd", "job:returns", Some("return")).await
    });

    let r1 = t1.await?;
    let r2 = t2.await?;

    let mut successes = 0;
    let mut rejections = 0;
    for r in [&r1, &r2] {
        match r {
            Ok(id) => {
                successes += 1;
                println!("  refund {id} accepted");
            }
            Err(RefundError::OverRefund {
                requested,
                remaining,
                ..
            }) => {
                rejections += 1;
                println!("  rejected: requested {requested} cents, only {remaining} remain");
            }
            Err(e) => println!("  unexpected error: {e}"),
        }
    }

    let total = model::refunded_so_far(&pool, charge.id).await?;
    println!(
        "outcome: {successes} accepted, {rejections} rejected; sum(refunds)={total} cents <= amount_captured={} cents",
        charge.amount_captured
    );

    assert_eq!(successes, 1, "exactly one refund must succeed");
    assert_eq!(rejections, 1, "the other must be rejected by the invariant");
    assert!(
        total <= charge.amount_captured,
        "sum(refunds) must never exceed amount_captured"
    );

    println!("OK: invariant held under concurrency (naive check-then-insert would have allowed 120_00 on a 100_00 capture)");
    Ok(())
}
