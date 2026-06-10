use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{RefundError, Result};

/// The refund state machine, mirroring the CHECK constraint on
/// `refunds.status`. `requested` -> `submitted` -> `settled` is the happy
/// path; `failed` and `canceled` are terminal off-ramps; `pending_review`
/// holds large refunds until a second person approves them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Requested,
    PendingReview,
    Submitted,
    Settled,
    Failed,
    Canceled,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Requested => "requested",
            Status::PendingReview => "pending_review",
            Status::Submitted => "submitted",
            Status::Settled => "settled",
            Status::Failed => "failed",
            Status::Canceled => "canceled",
        }
    }

    pub fn parse(s: &str) -> Option<Status> {
        Some(match s {
            "requested" => Status::Requested,
            "pending_review" => Status::PendingReview,
            "submitted" => Status::Submitted,
            "settled" => Status::Settled,
            "failed" => Status::Failed,
            "canceled" => Status::Canceled,
            _ => return None,
        })
    }

    /// Terminal states never move money again. `submit_refund` returns early
    /// on these, and the webhook handler refuses to resurrect them.
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Settled | Status::Failed | Status::Canceled)
    }
}

/// A row of the `refunds` table.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Refund {
    pub id: Uuid,
    pub charge_id: Uuid,
    pub amount_cents: i64,
    pub currency: String,
    pub idempotency_key: String,
    pub gateway_ref: Option<String>,
    pub status: String,
    pub reason_code: Option<String>,
    pub requested_by: String,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Refund {
    pub fn status_enum(&self) -> Status {
        Status::parse(&self.status).expect("status column constrained by CHECK")
    }
}

/// A row of the `charges` table.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Charge {
    pub id: Uuid,
    pub amount_captured: i64,
    pub currency: String,
}

/// Insert a charge. Helper for examples and tests; in production charges are
/// written by the capture path, not the refund engine.
pub async fn create_charge(
    pool: &PgPool,
    amount_captured: i64,
    currency: &str,
) -> Result<Charge> {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO charges (id, amount_captured, currency) VALUES ($1, $2, $3)",
    )
    .bind(id)
    .bind(amount_captured)
    .bind(currency)
    .execute(pool)
    .await?;
    Ok(Charge {
        id,
        amount_captured,
        currency: currency.to_string(),
    })
}

/// Fetch a refund by id.
pub async fn get_refund(pool: &PgPool, refund_id: Uuid) -> Result<Refund> {
    sqlx::query_as::<_, Refund>("SELECT * FROM refunds WHERE id = $1")
        .bind(refund_id)
        .fetch_optional(pool)
        .await?
        .ok_or(RefundError::RefundNotFound(refund_id))
}

/// Fetch a charge by id.
pub async fn get_charge(pool: &PgPool, charge_id: Uuid) -> Result<Charge> {
    sqlx::query_as::<_, Charge>(
        "SELECT id, amount_captured, currency FROM charges WHERE id = $1",
    )
    .bind(charge_id)
    .fetch_optional(pool)
    .await?
    .ok_or(RefundError::ChargeNotFound(charge_id))
}

/// Sum of refunds against a charge that moved (or may yet move) money.
/// Excludes `failed`/`canceled`, matching the over-refund invariant.
pub async fn refunded_so_far(pool: &PgPool, charge_id: Uuid) -> Result<i64> {
    let v: (i64,) = sqlx::query_as(
        "SELECT COALESCE(SUM(amount_cents), 0)::bigint FROM refunds \
         WHERE charge_id = $1 AND status NOT IN ('failed', 'canceled')",
    )
    .bind(charge_id)
    .fetch_one(pool)
    .await?;
    Ok(v.0)
}
