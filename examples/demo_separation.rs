//! Audit, approval, and PII (separation of duties).
//!
//! A large refund (at or above the review threshold) enters `pending_review`.
//! Approval BY THE REQUESTER is rejected. Approval by a different actor
//! succeeds and the refund becomes submittable. Also shows the PAN redaction
//! that keeps the last four and drops the rest before logging.

// Money literals read as dollars_cents, e.g. `5_000_00` = 5000 dollars 00 cents.
#![allow(clippy::inconsistent_digit_grouping)]

use refund_engine::error::RefundError;
use refund_engine::redact::redact_pan;
use refund_engine::{approval, connect_and_migrate, model, request, reset, submit, MockGateway};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&url).await?;
    reset(&pool).await?;

    // A large charge and a large refund (>= 1000.00 dollars threshold).
    let charge = model::create_charge(&pool, 5_000_00, "usd").await?;
    let requester = "user:alice";
    let refund_id = request::request_refund(
        &pool,
        charge.id,
        2_000_00, // 2000.00 dollars, over the review threshold
        "usd",
        requester,
        Some("large_goodwill"),
    )
    .await?;

    let status = model::get_refund(&pool, refund_id).await?.status;
    println!("large refund {refund_id} entered status={status}");
    assert_eq!(status, "pending_review", "large refunds must enter pending_review");

    // Self-approval is rejected.
    let self_approval = approval::approve(&pool, refund_id, requester).await;
    match &self_approval {
        Err(RefundError::SeparationOfDuties { approver }) => {
            println!("self-approval rejected: approver {approver} is the requester");
        }
        other => panic!("expected SeparationOfDuties, got {other:?}"),
    }

    // A different actor approves successfully.
    approval::approve(&pool, refund_id, "user:bob").await?;
    let after = model::get_refund(&pool, refund_id).await?.status;
    println!("approved by user:bob -> status={after}");
    assert_eq!(after, "requested", "approval moves pending_review -> requested");

    // Now it is submittable.
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, refund_id, &gateway).await?;
    assert_eq!(model::get_refund(&pool, refund_id).await?.status, "submitted");

    // PII: a PAN in a log line is redacted to last four.
    let line = "refund call with card 4242 4242 4242 4242 for charge";
    let redacted = redact_pan(line);
    println!("log redaction: {redacted}");
    assert!(!redacted.contains("4242 4242 4242 4242"), "full PAN must not survive");
    assert!(redacted.ends_with("4242 for charge"), "last four preserved");

    println!("OK: large refund gated on pending_review; self-approval rejected, third-party approval accepted; PAN redacted to last4");
    Ok(())
}
