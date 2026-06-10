//! Reconciliation is co-owned.
//!
//! Build a settlement file containing one of each discrepancy class and show
//! `reconcile` flags exactly those three: (1) settled in our DB but missing
//! from the file, (2) a settlement line with no matching refund row, and (3) an
//! amount mismatch. A clean settled refund that matches the file is flagged by
//! none of them.

use refund_engine::reconcile::{reconcile, SettlementLine};
use refund_engine::webhook::{handle_webhook, sign, WebhookData, WebhookEvent};
use refund_engine::{connect_and_migrate, model, request, reset, submit, MockGateway};

const SECRET: &[u8] = b"whsec_demo_secret";

async fn settle(pool: &sqlx::PgPool, gateway_ref: &str, evt_id: &str) {
    let ev = WebhookEvent {
        id: evt_id.to_string(),
        event_type: "refund.settled".to_string(),
        data: WebhookData { gateway_ref: gateway_ref.to_string(), reason: None },
    };
    let body = serde_json::to_vec(&ev).unwrap();
    let sig = sign(&body, SECRET);
    handle_webhook(pool, &ev, &body, &sig, SECRET).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&url).await?;
    reset(&pool).await?;

    let gateway = MockGateway::new();

    // Helper to make a settled refund and return (refund_id, gateway_ref, amount).
    async fn settled_refund(
        pool: &sqlx::PgPool,
        gateway: &MockGateway,
        amount: i64,
        tag: &str,
    ) -> (uuid::Uuid, String, i64) {
        let charge = model::create_charge(pool, amount * 2, "usd").await.unwrap();
        let id = request::request_refund(pool, charge.id, amount, "usd", "user:x", None)
            .await
            .unwrap();
        submit::submit_refund(pool, id, gateway).await.unwrap();
        let gref = model::get_refund(pool, id).await.unwrap().gateway_ref.unwrap();
        settle(pool, &gref, &format!("evt_{tag}")).await;
        (id, gref, amount)
    }

    // A: clean match (in DB settled AND in file with same amount).
    let (_a_id, a_ref, a_amt) = settled_refund(&pool, &gateway, 10_00, "a").await;
    // B: settled in DB, will be MISSING from the file (class one).
    let (b_id, _b_ref, _b_amt) = settled_refund(&pool, &gateway, 20_00, "b").await;
    // C: settled in DB, file has it but with a different amount (class three).
    let (c_id, c_ref, c_amt) = settled_refund(&pool, &gateway, 30_00, "c").await;

    // Build the settlement file:
    //   - A: matching line.
    //   - C: line with a wrong amount.
    //   - a ghost line with no refund row (class two).
    let file = vec![
        SettlementLine { gateway_ref: a_ref.clone(), amount_cents: a_amt },
        SettlementLine { gateway_ref: c_ref.clone(), amount_cents: c_amt + 1_00 }, // mismatch
        SettlementLine { gateway_ref: "re_ghost_00000001".to_string(), amount_cents: 99_00 },
    ];

    let result = reconcile(&pool, &file).await?;

    println!("class 1 (settled in DB, missing from file): {:?}", result.settled_missing_from_file);
    println!("class 2 (line with no refund row):          {:?}", result.line_with_no_refund);
    println!(
        "class 3 (amount mismatch):                   {:?}",
        result
            .amount_mismatch
            .iter()
            .map(|m| (m.refund_id, m.db_amount_cents, m.file_amount_cents))
            .collect::<Vec<_>>()
    );

    assert_eq!(result.settled_missing_from_file, vec![b_id], "class one = refund B");
    assert_eq!(result.line_with_no_refund, vec!["re_ghost_00000001".to_string()], "class two = ghost line");
    assert_eq!(result.amount_mismatch.len(), 1, "class three = one mismatch");
    assert_eq!(result.amount_mismatch[0].refund_id, c_id, "class three = refund C");

    println!("OK: reconcile flagged exactly one of each discrepancy class; the clean match (A) was flagged by none");
    Ok(())
}
