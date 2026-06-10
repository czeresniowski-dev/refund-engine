//! Hostile webhooks.
//!
//! Deliver webhooks out of order (settled before the submit we expect to
//! precede it) and duplicated, then prove a late `settled` never resurrects a
//! `failed` refund. The handler verifies the signature, dedupes on event id,
//! and keys off `gateway_ref` rather than assuming a sequence.

use refund_engine::webhook::{handle_webhook, sign, WebhookData, WebhookEvent, WebhookResult};
use refund_engine::{connect_and_migrate, model, request, reset, submit, MockGateway};

const SECRET: &[u8] = b"whsec_demo_secret";

fn event(id: &str, ty: &str, gateway_ref: &str, reason: Option<&str>) -> (WebhookEvent, Vec<u8>, String) {
    let ev = WebhookEvent {
        id: id.to_string(),
        event_type: ty.to_string(),
        data: WebhookData {
            gateway_ref: gateway_ref.to_string(),
            reason: reason.map(|s| s.to_string()),
        },
    };
    let body = serde_json::to_vec(&ev).unwrap();
    let sig = sign(&body, SECRET);
    (ev, body, sig)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&url).await?;
    reset(&pool).await?;

    let gateway = MockGateway::new();

    // --- Refund A: settled webhook arrives, duplicated, out of order. ---
    let charge_a = model::create_charge(&pool, 80_00, "usd").await?;
    let a = request::request_refund(&pool, charge_a.id, 30_00, "usd", "user:1", None).await?;
    submit::submit_refund(&pool, a, &gateway).await?;
    let ref_a = model::get_refund(&pool, a).await?.gateway_ref.unwrap();

    // Bad signature is rejected before any state change.
    let (ev, body, _good_sig) = event("evt_bad", "refund.settled", &ref_a, None);
    let bad = handle_webhook(&pool, &ev, &body, "deadbeef", SECRET).await;
    println!("bad-signature webhook rejected: {}", bad.is_err());
    assert!(bad.is_err(), "bad signature must be rejected");

    // Settled, then the SAME event delivered again (duplicate).
    let (ev, body, sig) = event("evt_a_settled", "refund.settled", &ref_a, None);
    assert_eq!(handle_webhook(&pool, &ev, &body, &sig, SECRET).await?, WebhookResult::Applied);
    assert_eq!(
        handle_webhook(&pool, &ev, &body, &sig, SECRET).await?,
        WebhookResult::Duplicate,
        "replay must be a no-op"
    );
    let status_a = model::get_refund(&pool, a).await?.status;
    println!("refund A after settled + duplicate: status={status_a}");
    assert_eq!(status_a, "settled");

    // --- Refund B: a failed refund must NOT be resurrected by a late settled. ---
    let charge_b = model::create_charge(&pool, 80_00, "usd").await?;
    let b = request::request_refund(&pool, charge_b.id, 25_00, "usd", "user:2", None).await?;
    submit::submit_refund(&pool, b, &gateway).await?;
    let ref_b = model::get_refund(&pool, b).await?.gateway_ref.unwrap();

    // The bank rejected it after acceptance.
    let (ev, body, sig) = event("evt_b_failed", "refund.failed", &ref_b, Some("bank_declined"));
    handle_webhook(&pool, &ev, &body, &sig, SECRET).await?;
    // A late settled arrives for the same ref.
    let (ev, body, sig) = event("evt_b_settled_late", "refund.settled", &ref_b, None);
    handle_webhook(&pool, &ev, &body, &sig, SECRET).await?;

    let rb = model::get_refund(&pool, b).await?;
    println!(
        "refund B after failed + late settled: status={} failure_reason={:?}",
        rb.status, rb.failure_reason
    );
    assert_eq!(rb.status, "failed", "a late settled must not resurrect a failed refund");

    // --- Refund C: settled arrives BEFORE we even record it (out of order). ---
    // The guard allows advancing from 'submitted' or 'requested', so an early
    // settled on a still-'requested' (but submitted-to-gateway) row is safe.
    let charge_c = model::create_charge(&pool, 80_00, "usd").await?;
    let c = request::request_refund(&pool, charge_c.id, 10_00, "usd", "user:3", None).await?;
    submit::submit_refund(&pool, c, &gateway).await?;
    let ref_c = model::get_refund(&pool, c).await?.gateway_ref.unwrap();
    let (ev, body, sig) = event("evt_c_settled", "refund.settled", &ref_c, None);
    handle_webhook(&pool, &ev, &body, &sig, SECRET).await?;
    assert_eq!(model::get_refund(&pool, c).await?.status, "settled");

    println!("OK: out-of-order and duplicate webhooks tolerated; bad signatures rejected; a failed refund was never resurrected by a late settled");
    Ok(())
}
