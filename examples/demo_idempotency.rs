//! Idempotency keys done right.
//!
//! Submit a refund, then RETRY the same submit with the same idempotency key
//! (the refund's own uuid id). The gateway dedupes on the key, so it issues
//! exactly ONE refund no matter how many times we retry.

use refund_engine::{connect_and_migrate, model, request, reset, submit, MockGateway};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&url).await?;
    reset(&pool).await?;

    let charge = model::create_charge(&pool, 50_00, "usd").await?;
    let refund_id =
        request::request_refund(&pool, charge.id, 20_00, "usd", "user:42", Some("goodwill"))
            .await?;
    println!("requested refund {refund_id}");

    let gateway = MockGateway::new();

    // First submit.
    submit::submit_refund(&pool, refund_id, &gateway).await?;
    // Retry the same submit three more times (crash-and-retry, same key).
    for _ in 0..3 {
        submit::submit_refund(&pool, refund_id, &gateway).await?;
    }

    let issued = gateway.issued_count();
    let refund = model::get_refund(&pool, refund_id).await?;
    println!(
        "gateway issued {issued} refund(s); db gateway_ref={:?} status={}",
        refund.gateway_ref, refund.status
    );

    assert_eq!(issued, 1, "gateway must issue exactly one refund across retries");
    assert!(refund.gateway_ref.is_some(), "gateway_ref must be recorded");
    assert_eq!(refund.status, "submitted", "must NOT be settled from optimism");

    println!("OK: 4 submit calls, gateway issued exactly 1 refund; row stayed 'submitted' (never settled on optimism)");
    Ok(())
}
