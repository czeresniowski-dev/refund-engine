//! Assertion versions of every demo. Run serial:
//!   DATABASE_URL=... cargo test -- --test-threads=1
//!
//! Each test migrates and resets its own tables, so the suite is idempotent on
//! re-run.

// Money literals read as dollars_cents, e.g. `100_00` = 100 dollars 00 cents.
#![allow(clippy::inconsistent_digit_grouping)]

use std::sync::Arc;

use refund_engine::error::RefundError;
use refund_engine::reconcile::{reconcile, SettlementLine};
use refund_engine::redact::{last4, redact_pan};
use refund_engine::webhook::{handle_webhook, sign, verify_signature, WebhookData, WebhookEvent, WebhookResult};
use refund_engine::{
    approval, connect_and_migrate, model, recover, request, reset, submit, MockConfig, MockGateway,
};
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for DB tests");
    let pool = connect_and_migrate(&url).await.expect("connect + migrate");
    reset(&pool).await.expect("reset");
    pool
}

// ---- Preventing over-refund under concurrency ----

#[tokio::test]
async fn over_refund_under_concurrency() {
    let pool = Arc::new(pool().await);
    let charge = model::create_charge(&pool, 100_00, "usd").await.unwrap();

    let p1 = pool.clone();
    let p2 = pool.clone();
    let cid = charge.id;
    let t1 = tokio::spawn(async move {
        request::request_refund(&p1, cid, 60_00, "usd", "user:agent", None).await
    });
    let t2 = tokio::spawn(async move {
        request::request_refund(&p2, cid, 60_00, "usd", "job:returns", None).await
    });
    let (r1, r2) = (t1.await.unwrap(), t2.await.unwrap());

    let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let over = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(RefundError::OverRefund { .. })))
        .count();
    assert_eq!(successes, 1, "exactly one refund succeeds");
    assert_eq!(over, 1, "the other is rejected by the invariant");

    let total = model::refunded_so_far(&pool, charge.id).await.unwrap();
    assert!(total <= charge.amount_captured);
    assert_eq!(total, 60_00);
}

#[tokio::test]
async fn currency_mismatch_rejected() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 100_00, "usd").await.unwrap();
    let r = request::request_refund(&pool, charge.id, 10_00, "eur", "user:1", None).await;
    assert!(matches!(r, Err(RefundError::CurrencyMismatch { .. })));
}

// ---- Idempotency keys done right ----

#[tokio::test]
async fn idempotency_one_refund_across_retries() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 20_00, "usd", "user:1", None)
        .await
        .unwrap();

    let gateway = MockGateway::new();
    for _ in 0..5 {
        submit::submit_refund(&pool, id, &gateway).await.unwrap();
    }
    assert_eq!(gateway.issued_count(), 1, "exactly one refund issued");

    let r = model::get_refund(&pool, id).await.unwrap();
    assert_eq!(r.status, "submitted", "never settled from optimism");
    assert!(r.gateway_ref.is_some());

    // The idempotency key is the refund's own id.
    assert_eq!(r.idempotency_key, id.to_string());
}

#[tokio::test]
async fn submit_skips_terminal_rows() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", None)
        .await
        .unwrap();
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, id, &gateway).await.unwrap();

    // Settle it via webhook, then a re-submit must be a no-op.
    let gref = model::get_refund(&pool, id).await.unwrap().gateway_ref.unwrap();
    settle(&pool, &gref, "evt_skip").await;
    assert_eq!(model::get_refund(&pool, id).await.unwrap().status, "settled");

    let accepted = submit::submit_refund(&pool, id, &gateway).await.unwrap();
    assert!(!accepted, "terminal rows are skipped");
    assert_eq!(gateway.issued_count(), 1, "no second issue on a settled row");
}

// ---- Hostile webhooks ----

const SECRET: &[u8] = b"whsec_test_secret";

async fn settle(pool: &PgPool, gateway_ref: &str, evt_id: &str) {
    let ev = WebhookEvent {
        id: evt_id.to_string(),
        event_type: "refund.settled".to_string(),
        data: WebhookData { gateway_ref: gateway_ref.to_string(), reason: None },
    };
    let body = serde_json::to_vec(&ev).unwrap();
    let sig = sign(&body, SECRET);
    handle_webhook(pool, &ev, &body, &sig, SECRET).await.unwrap();
}

fn make_event(id: &str, ty: &str, gref: &str, reason: Option<&str>) -> (WebhookEvent, Vec<u8>, String) {
    let ev = WebhookEvent {
        id: id.to_string(),
        event_type: ty.to_string(),
        data: WebhookData { gateway_ref: gref.to_string(), reason: reason.map(String::from) },
    };
    let body = serde_json::to_vec(&ev).unwrap();
    let sig = sign(&body, SECRET);
    (ev, body, sig)
}

#[tokio::test]
async fn webhook_signature_required() {
    assert!(verify_signature(b"hello", &sign(b"hello", SECRET), SECRET));
    assert!(!verify_signature(b"hello", "00", SECRET));
    assert!(!verify_signature(b"hello", &sign(b"hello", b"other"), SECRET));
}

#[tokio::test]
async fn webhook_bad_signature_rejected_before_state_change() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", None)
        .await
        .unwrap();
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, id, &gateway).await.unwrap();
    let gref = model::get_refund(&pool, id).await.unwrap().gateway_ref.unwrap();

    let (ev, body, _) = make_event("evt_bad", "refund.settled", &gref, None);
    let r = handle_webhook(&pool, &ev, &body, "deadbeef", SECRET).await;
    assert!(matches!(r, Err(RefundError::BadSignature)));
    assert_eq!(model::get_refund(&pool, id).await.unwrap().status, "submitted");
}

#[tokio::test]
async fn webhook_duplicate_is_noop() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", None)
        .await
        .unwrap();
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, id, &gateway).await.unwrap();
    let gref = model::get_refund(&pool, id).await.unwrap().gateway_ref.unwrap();

    let (ev, body, sig) = make_event("evt_dup", "refund.settled", &gref, None);
    assert_eq!(handle_webhook(&pool, &ev, &body, &sig, SECRET).await.unwrap(), WebhookResult::Applied);
    assert_eq!(handle_webhook(&pool, &ev, &body, &sig, SECRET).await.unwrap(), WebhookResult::Duplicate);
    assert_eq!(model::get_refund(&pool, id).await.unwrap().status, "settled");
}

#[tokio::test]
async fn webhook_late_settled_does_not_resurrect_failed() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", None)
        .await
        .unwrap();
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, id, &gateway).await.unwrap();
    let gref = model::get_refund(&pool, id).await.unwrap().gateway_ref.unwrap();

    let (ev, body, sig) = make_event("evt_fail", "refund.failed", &gref, Some("bank_declined"));
    handle_webhook(&pool, &ev, &body, &sig, SECRET).await.unwrap();
    let (ev, body, sig) = make_event("evt_late_settle", "refund.settled", &gref, None);
    handle_webhook(&pool, &ev, &body, &sig, SECRET).await.unwrap();

    let r = model::get_refund(&pool, id).await.unwrap();
    assert_eq!(r.status, "failed", "a late settled must not resurrect a failed refund");
    assert_eq!(r.failure_reason.as_deref(), Some("bank_declined"));
}

// ---- The 12k incident ----

#[tokio::test]
async fn incident_zero_double_refunds() {
    let pool = pool().await;
    let gateway = MockGateway::new();

    const N: usize = 200;
    const KILL_AT: usize = 120;
    const NEVER_AT: usize = KILL_AT + 60;

    let mut ids = Vec::new();
    for _ in 0..N {
        let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
        ids.push(
            request::request_refund(&pool, charge.id, 20_00, "usd", "job:carrier", None)
                .await
                .unwrap(),
        );
    }

    for (i, id) in ids.iter().enumerate() {
        if i == KILL_AT {
            gateway.set_config(MockConfig { drop_response: true, ..Default::default() });
        }
        if i >= NEVER_AT {
            continue; // worker dead; row stays 'requested'
        }
        submit::submit_refund(&pool, *id, &gateway).await.unwrap();
    }

    let settled: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM refunds WHERE status='settled'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(settled.0, 0, "nothing settled before recovery");

    // The worker comes back and we run the runbook against a healthy gateway:
    // stop dropping responses so recovery can record refs and resubmit cleanly.
    gateway.set_config(MockConfig::default());
    let report = recover::recover_submitted(&pool, &gateway).await.unwrap();
    let issued = gateway.issued_count();

    let distinct: (i64,) =
        sqlx::query_as("SELECT COUNT(DISTINCT idempotency_key) FROM refunds")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(distinct.0, N as i64);
    assert!(issued as i64 <= N as i64, "gateway never issues more than N refunds");
    // zero double-refunds: issued count equals distinct keys that were submitted.
    assert_eq!(issued as i64, distinct.0, "every distinct refund issued exactly once");

    let unconverged: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM refunds WHERE gateway_ref IS NULL OR status NOT IN ('submitted','settled','failed','canceled')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unconverged.0, 0, "every row converged");
    assert_eq!(report.walked as i64, N as i64, "all stuck rows walked");
}

#[tokio::test]
async fn recovery_does_not_resubmit_already_accepted() {
    // A single row submitted with a dropped response: recovery must find it at
    // the gateway by key and NOT issue a second refund.
    let pool = pool().await;
    let gateway = MockGateway::with_config(MockConfig { drop_response: true, ..Default::default() });
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", None)
        .await
        .unwrap();
    let accepted = submit::submit_refund(&pool, id, &gateway).await.unwrap();
    assert!(!accepted, "response was dropped");
    assert!(model::get_refund(&pool, id).await.unwrap().gateway_ref.is_none());
    assert_eq!(gateway.issued_count(), 1, "gateway accepted once");

    // Stop dropping so recovery can record the ref; the lookup still finds it.
    gateway.set_config(MockConfig::default());
    recover::recover_submitted(&pool, &gateway).await.unwrap();

    assert_eq!(gateway.issued_count(), 1, "recovery must not double-refund");
    let r = model::get_refund(&pool, id).await.unwrap();
    assert!(r.gateway_ref.is_some(), "recovery recorded the gateway_ref");
    assert_eq!(r.status, "submitted", "left for the settle webhook");
}

// ---- Audit, approval, and PII ----

#[tokio::test]
async fn separation_of_duties() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 5_000_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 2_000_00, "usd", "user:alice", None)
        .await
        .unwrap();
    assert_eq!(model::get_refund(&pool, id).await.unwrap().status, "pending_review");

    // self-approval rejected
    assert!(matches!(
        approval::approve(&pool, id, "user:alice").await,
        Err(RefundError::SeparationOfDuties { .. })
    ));
    // third-party approval succeeds
    approval::approve(&pool, id, "user:bob").await.unwrap();
    assert_eq!(model::get_refund(&pool, id).await.unwrap().status, "requested");
}

#[tokio::test]
async fn transitions_are_appended() {
    let pool = pool().await;
    let charge = model::create_charge(&pool, 50_00, "usd").await.unwrap();
    let id = request::request_refund(&pool, charge.id, 10_00, "usd", "user:1", Some("goodwill"))
        .await
        .unwrap();
    let gateway = MockGateway::new();
    submit::submit_refund(&pool, id, &gateway).await.unwrap();
    let gref = model::get_refund(&pool, id).await.unwrap().gateway_ref.unwrap();
    settle(&pool, &gref, "evt_trans").await;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT COALESCE(from_status,'NULL'), to_status FROM refund_transitions WHERE refund_id=$1 ORDER BY id",
    )
    .bind(id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let seq: Vec<(String, String)> = rows;
    assert_eq!(
        seq,
        vec![
            ("NULL".to_string(), "requested".to_string()),
            ("requested".to_string(), "submitted".to_string()),
            ("submitted".to_string(), "settled".to_string()),
        ],
        "append-only transition history reconstructs the state machine"
    );
}

#[tokio::test]
async fn pan_redaction() {
    assert_eq!(redact_pan("card 4242 4242 4242 4242 done"), "card **** **** **** 4242 done");
    assert_eq!(redact_pan("pan 4242424242424242"), "pan ************4242");
    assert_eq!(redact_pan("hyphen 4111-1111-1111-1111!"), "hyphen ****-****-****-1111!");
    // A 4-digit order number is not a PAN and survives untouched.
    assert_eq!(redact_pan("order 1234"), "order 1234");
    assert_eq!(last4("4242 4242 4242 4242").as_deref(), Some("4242"));
    assert_eq!(last4("12"), None);
}

// ---- Reconciliation is co-owned ----

#[tokio::test]
async fn reconcile_three_classes() {
    let pool = pool().await;
    let gateway = MockGateway::new();

    async fn settled(pool: &PgPool, g: &MockGateway, amount: i64, tag: &str) -> (Uuid, String) {
        let charge = model::create_charge(pool, amount * 2, "usd").await.unwrap();
        let id = request::request_refund(pool, charge.id, amount, "usd", "user:x", None)
            .await
            .unwrap();
        submit::submit_refund(pool, id, g).await.unwrap();
        let gref = model::get_refund(pool, id).await.unwrap().gateway_ref.unwrap();
        let ev = WebhookEvent {
            id: format!("evt_{tag}"),
            event_type: "refund.settled".to_string(),
            data: WebhookData { gateway_ref: gref.clone(), reason: None },
        };
        let body = serde_json::to_vec(&ev).unwrap();
        let sig = sign(&body, SECRET);
        handle_webhook(pool, &ev, &body, &sig, SECRET).await.unwrap();
        (id, gref)
    }

    let (_a, a_ref) = settled(&pool, &gateway, 10_00, "a").await; // clean
    let (b, _b_ref) = settled(&pool, &gateway, 20_00, "b").await; // missing from file
    let (c, c_ref) = settled(&pool, &gateway, 30_00, "c").await; // amount mismatch

    let file = vec![
        SettlementLine { gateway_ref: a_ref, amount_cents: 10_00 },
        SettlementLine { gateway_ref: c_ref, amount_cents: 31_00 },
        SettlementLine { gateway_ref: "re_ghost".to_string(), amount_cents: 99_00 },
    ];

    let r = reconcile(&pool, &file).await.unwrap();
    assert_eq!(r.settled_missing_from_file, vec![b]);
    assert_eq!(r.line_with_no_refund, vec!["re_ghost".to_string()]);
    assert_eq!(r.amount_mismatch.len(), 1);
    assert_eq!(r.amount_mismatch[0].refund_id, c);
    assert_eq!(r.amount_mismatch[0].db_amount_cents, 30_00);
    assert_eq!(r.amount_mismatch[0].file_amount_cents, 31_00);
}
