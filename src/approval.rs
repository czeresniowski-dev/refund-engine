use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{RefundError, Result};
use crate::model::{get_refund, Status};
use crate::submit::record_transition;

/// Approve a refund sitting in `pending_review`, moving it to `requested` so it
/// becomes submittable. Separation of duties is enforced in code: the approver
/// must not be the requester. This stops both an honest fat-finger refund of
/// 50,000 instead of 500 and a dishonest employee quietly refunding to an
/// account they control.
pub async fn approve(pool: &PgPool, refund_id: Uuid, approver: &str) -> Result<()> {
    let refund = get_refund(pool, refund_id).await?;

    if refund.status_enum() != Status::PendingReview {
        return Err(RefundError::NotPendingReview(refund_id));
    }

    // approver != requested_by, or reject.
    if approver == refund.requested_by {
        return Err(RefundError::SeparationOfDuties {
            approver: approver.to_string(),
        });
    }

    // Guarded so a concurrent approval can't double-advance.
    sqlx::query(
        "UPDATE refunds SET status='requested', updated_at=now() \
         WHERE id=$1 AND status='pending_review'",
    )
    .bind(refund_id)
    .execute(pool)
    .await?;

    // The approver lands in the append-only audit table: "who authorized this
    // money to move and when" is a query, not an archaeology project.
    record_transition(
        pool,
        refund_id,
        Some("pending_review"),
        "requested",
        &format!("approver:{approver}"),
    )
    .await?;

    Ok(())
}
