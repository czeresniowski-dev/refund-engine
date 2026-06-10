use sqlx::PgPool;
use uuid::Uuid;

use crate::error::Result;
use crate::gateway::Gateway;
use crate::submit::record_transition;

/// What recovery did to one stuck row, so the headline can be reported.
#[derive(Debug, Default, Clone, Copy)]
pub struct RecoveryReport {
    /// Rows the gateway already held (idempotency-key hit). We recorded the
    /// gateway_ref and left them for the settle webhook. No resubmission.
    pub already_at_gateway: u64,
    /// Rows the gateway did not have. We resubmitted safely with the same key.
    pub resubmitted: u64,
    /// Total `submitted` (or stuck `requested`) rows walked.
    pub walked: u64,
}

/// The 12k-incident converge-don't-guess procedure. For every row stuck in
/// `submitted` (and any `requested` left mid-batch), ask the gateway BY
/// IDEMPOTENCY KEY (= the refund id) whether it already holds the refund:
///
/// - If yes, it had already happened: record the `gateway_ref`, leave it for
///   the settle webhook, do NOT resubmit.
/// - If no, it hadn't happened: resubmit safely with the same key. If our
///   judgment is wrong and it had gone through, the gateway's dedupe returns
///   the original rather than moving money twice.
///
/// We never advance a row to `settled` here. Recovery converges intent and
/// gateway state; settlement still comes only from the webhook.
pub async fn recover_submitted<G: Gateway>(pool: &PgPool, gateway: &G) -> Result<RecoveryReport> {
    let mut report = RecoveryReport::default();

    // Walk every non-terminal row that could be stuck in the crash window.
    // 'submitted' is the dangerous class; 'requested' covers rows the batch
    // never got to call the gateway for before it died.
    let stuck: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, status FROM refunds \
         WHERE status IN ('submitted','requested') \
         ORDER BY created_at",
    )
    .fetch_all(pool)
    .await?;

    for (refund_id, status) in stuck {
        report.walked += 1;
        let key = refund_id.to_string();

        // The converge-don't-guess question: do you already hold a refund with
        // this idempotency key?
        if let Some(gw) = gateway.lookup_by_key(&key) {
            // Yes. Record the ref if we don't already have it, and leave it for
            // the settle webhook. Make sure the row is marked submitted.
            sqlx::query(
                "UPDATE refunds SET gateway_ref=$2, status='submitted', updated_at=now() \
                 WHERE id=$1 AND gateway_ref IS NULL",
            )
            .bind(refund_id)
            .bind(&gw.id)
            .execute(pool)
            .await?;
            record_transition(
                pool,
                refund_id,
                Some(&status),
                "submitted",
                "job:recover",
            )
            .await?;
            report.already_at_gateway += 1;
        } else {
            // No. Resubmit safely with the same key. The gateway's dedupe makes
            // this safe even if our judgment is wrong.
            let row: (i64, String) = sqlx::query_as(
                "SELECT amount_cents, currency FROM refunds WHERE id=$1",
            )
            .bind(refund_id)
            .fetch_one(pool)
            .await?;

            // Ensure the row is 'submitted' before the call (mirror submit).
            sqlx::query(
                "UPDATE refunds SET status='submitted', updated_at=now() \
                 WHERE id=$1 AND status IN ('requested','submitted')",
            )
            .bind(refund_id)
            .execute(pool)
            .await?;

            let outcome = gateway.create_refund(row.0, &row.1, &key);
            if let crate::gateway::GatewayOutcome::Accepted(gw) = outcome {
                sqlx::query(
                    "UPDATE refunds SET gateway_ref=$2, updated_at=now() \
                     WHERE id=$1 AND gateway_ref IS NULL",
                )
                .bind(refund_id)
                .bind(&gw.id)
                .execute(pool)
                .await?;
            }
            record_transition(
                pool,
                refund_id,
                Some(&status),
                "submitted",
                "job:recover-resubmit",
            )
            .await?;
            report.resubmitted += 1;
        }
    }

    Ok(report)
}
