use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{RefundError, Result};
use crate::model::Status;

/// Amounts at or above this threshold (in cents) enter `pending_review` and
/// must be approved by a second person before they can reach the gateway.
/// The article's "50,000 instead of 500" fat-finger lives above this line.
pub const REVIEW_THRESHOLD_CENTS: i64 = 100_000;

/// Request a refund against a charge, enforcing `sum(refunds) <=
/// amount_captured` under a row lock so the check is a true invariant rather
/// than a check-then-insert race.
///
/// This is the `BEGIN; SELECT ... FOR UPDATE; SELECT SUM(...); assert;
/// INSERT refund; INSERT transition; COMMIT` sequence from the article,
/// verbatim. The `idempotency_key` is the refund's OWN uuid id, allocated
/// here and persisted before any gateway call.
pub async fn request_refund(
    pool: &PgPool,
    charge_id: Uuid,
    amount_cents: i64,
    currency: &str,
    actor: &str,
    reason_code: Option<&str>,
) -> Result<Uuid> {
    let refund_id = Uuid::new_v4();

    let mut tx = pool.begin().await?;

    // Lock the charge row. Any concurrent refund against this charge now
    // blocks here until we commit. This is what makes the invariant real.
    let charge: Option<(i64, String)> = sqlx::query_as(
        "SELECT amount_captured, currency FROM charges WHERE id = $1 FOR UPDATE",
    )
    .bind(charge_id)
    .fetch_optional(&mut *tx)
    .await?;

    let (amount_captured, charge_currency) =
        charge.ok_or(RefundError::ChargeNotFound(charge_id))?;

    // With the row locked, this sum cannot change underneath us.
    // Exclude failed/canceled refunds; they never moved money.
    let refunded_so_far: (i64,) = sqlx::query_as(
        // Cast to bigint: SUM(bigint) is NUMERIC in Postgres; the article's
        // arithmetic is integer cents, so we decode it back to bigint.
        "SELECT COALESCE(SUM(amount_cents), 0)::bigint AS refunded_so_far \
           FROM refunds \
          WHERE charge_id = $1 \
            AND status NOT IN ('failed', 'canceled')",
    )
    .bind(charge_id)
    .fetch_one(&mut *tx)
    .await?;
    let refunded_so_far = refunded_so_far.0;

    // assert: $new_amount_cents <= amount_captured - refunded_so_far
    //         and $new_currency = charges.currency
    if currency != charge_currency {
        return Err(RefundError::CurrencyMismatch {
            refund: currency.to_string(),
            charge: charge_currency,
        });
    }
    let remaining = amount_captured - refunded_so_far;
    if amount_cents > remaining {
        // Rolling back is implicit on drop, but be explicit for clarity.
        tx.rollback().await?;
        return Err(RefundError::OverRefund {
            charge_id,
            requested: amount_cents,
            remaining,
        });
    }

    // Amounts over the threshold start in pending_review instead of requested,
    // so a second person approves the large ones before they reach the gateway.
    let initial_status = if amount_cents >= REVIEW_THRESHOLD_CENTS {
        Status::PendingReview
    } else {
        Status::Requested
    };

    // INSERT the refund. idempotency_key = the refund's OWN uuid id.
    sqlx::query(
        "INSERT INTO refunds (id, charge_id, amount_cents, currency, \
                              idempotency_key, reason_code, requested_by, status) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(refund_id)
    .bind(charge_id)
    .bind(amount_cents)
    .bind(currency)
    .bind(refund_id.to_string()) // idempotency_key IS the refund id
    .bind(reason_code)
    .bind(actor)
    .bind(initial_status.as_str())
    .execute(&mut *tx)
    .await?;

    // Same transaction: the audit row. State and transition commit together
    // or not at all, so they can never disagree.
    sqlx::query(
        "INSERT INTO refund_transitions (refund_id, from_status, to_status, actor, reason) \
         VALUES ($1, NULL, $2, $3, $4)",
    )
    .bind(refund_id)
    .bind(initial_status.as_str())
    .bind(actor)
    .bind(reason_code)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(refund_id)
}
