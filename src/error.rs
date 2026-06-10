use thiserror::Error;

/// Errors the refund engine raises. The money-moving ones (`OverRefund`,
/// `CurrencyMismatch`, `SeparationOfDuties`) are deliberately distinct
/// variants so a caller can branch on them rather than parse a string.
#[derive(Debug, Error)]
pub enum RefundError {
    /// The new refund would push `sum(refunds) > amount_captured`. Raised
    /// while holding the `FOR UPDATE` lock on the charge row, so it is the
    /// true invariant, not a stale-read advisory.
    #[error("over-refund: requested {requested} cents but only {remaining} cents remain on charge {charge_id}")]
    OverRefund {
        charge_id: uuid::Uuid,
        requested: i64,
        remaining: i64,
    },

    /// A refund in a different currency than the capture is a second
    /// unhedged FX transaction wearing a refund's clothes.
    #[error("currency mismatch: refund {refund} != charge {charge}")]
    CurrencyMismatch { refund: String, charge: String },

    /// The charge the refund points at does not exist.
    #[error("charge {0} not found")]
    ChargeNotFound(uuid::Uuid),

    /// The refund id does not exist.
    #[error("refund {0} not found")]
    RefundNotFound(uuid::Uuid),

    /// The approver is the same actor that requested the refund. Separation
    /// of duties forbids self-approval of money movement.
    #[error("separation of duties: approver {approver} is the requester")]
    SeparationOfDuties { approver: String },

    /// The refund is not in `pending_review`, so there is nothing to approve.
    #[error("refund {0} is not pending review")]
    NotPendingReview(uuid::Uuid),

    /// The webhook signature did not verify. Reject before any state change.
    #[error("bad webhook signature")]
    BadSignature,

    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, RefundError>;
