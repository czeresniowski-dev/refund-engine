use sqlx::PgPool;
use uuid::Uuid;

use crate::error::Result;
use crate::gateway::{Gateway, GatewayOutcome};
use crate::model::{get_refund, Status};

/// Submit a previously requested refund to the gateway. This is the article's
/// `submit_refund` ported verbatim: skip if terminal; guard the UPDATE to
/// `submitted` by `status IN ('requested','submitted')`; call the gateway with
/// `idempotency_key = refund_id`; record the `gateway_ref` but NEVER set
/// `settled` here. Settlement is the gateway's word, by webhook or poll.
///
/// Returns `true` if the gateway accepted and we recorded a `gateway_ref`,
/// `false` if the call was dropped/timed out (the row stays `submitted` with a
/// null `gateway_ref`, exactly the state recovery converges).
pub async fn submit_refund<G: Gateway>(
    pool: &PgPool,
    refund_id: Uuid,
    gateway: &G,
) -> Result<bool> {
    // refund_id was persisted as status='requested' (or 'pending_review' then
    // approved) inside the FOR UPDATE transaction. It IS the idempotency key.
    let row = get_refund(pool, refund_id).await?;

    let status = row.status_enum();
    if matches!(status, Status::Settled | Status::Failed | Status::Canceled) {
        return Ok(false); // already terminal
    }

    // A pending_review refund must be approved first; it is not submittable.
    if status == Status::PendingReview {
        return Ok(false);
    }

    // Guarded transition to submitted. Idempotent on retry: a row already
    // 'submitted' stays 'submitted'.
    let from_status = row.status.clone();
    sqlx::query(
        "UPDATE refunds SET status='submitted', updated_at=now() \
         WHERE id=$1 AND status IN ('requested','submitted')",
    )
    .bind(refund_id)
    .execute(pool)
    .await?;

    if from_status != Status::Submitted.as_str() {
        record_transition(pool, refund_id, Some(&from_status), "submitted", "job:submit").await?;
    }

    // A retry of a call that already succeeded returns the SAME refund object
    // instead of moving money again. A dropped/timed-out response leaves the
    // row 'submitted' with no gateway_ref, to be converged by recovery.
    let outcome = gateway.create_refund(row.amount_cents, &row.currency, &refund_id.to_string());

    let gw = match outcome {
        GatewayOutcome::Accepted(gw) => gw,
        GatewayOutcome::Dropped | GatewayOutcome::TimedOut => return Ok(false),
    };

    // Record the gateway ref, but DO NOT mark settled here. A 2xx means the
    // gateway accepted the request, not that the bank moved the money.
    sqlx::query(
        "UPDATE refunds SET gateway_ref=$2, updated_at=now() \
         WHERE id=$1 AND gateway_ref IS NULL",
    )
    .bind(refund_id)
    .bind(&gw.id)
    .execute(pool)
    .await?;

    Ok(true)
}

/// Append a transition row. Always append-only; never UPDATE or DELETE.
pub(crate) async fn record_transition(
    pool: &PgPool,
    refund_id: Uuid,
    from_status: Option<&str>,
    to_status: &str,
    actor: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO refund_transitions (refund_id, from_status, to_status, actor) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(refund_id)
    .bind(from_status)
    .bind(to_status)
    .bind(actor)
    .execute(pool)
    .await?;
    Ok(())
}
