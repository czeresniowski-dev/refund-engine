use std::collections::HashMap;

use sqlx::PgPool;
use uuid::Uuid;

use crate::error::Result;

/// One line of the bank's settlement file: the gateway ref and the amount the
/// bank says actually moved. The file, not the database, is the number that
/// matters.
#[derive(Debug, Clone)]
pub struct SettlementLine {
    pub gateway_ref: String,
    pub amount_cents: i64,
}

/// The three discrepancy classes the daily reconciliation job flags. Each maps
/// to a distinct operational response; class two is a finance escalation with a
/// clock on it.
#[derive(Debug, Default)]
pub struct ReconResult {
    /// `settled` in our DB but missing from the file. The bank dropped it and a
    /// customer waits on money we think we sent.
    pub settled_missing_from_file: Vec<Uuid>,
    /// A settlement line with no matching refund row. Money left the account we
    /// can't account for: the worst class, a finance escalation.
    pub line_with_no_refund: Vec<String>,
    /// Amount mismatch between our settled row and the file. Almost always a
    /// currency, rounding, or partial-capture bug.
    pub amount_mismatch: Vec<AmountMismatch>,
}

#[derive(Debug, Clone)]
pub struct AmountMismatch {
    pub gateway_ref: String,
    pub refund_id: Uuid,
    pub db_amount_cents: i64,
    pub file_amount_cents: i64,
}

/// Match each `settled` refund against the settlement file by `gateway_ref`
/// and amount, flagging the three discrepancy classes. The job compares our
/// story to the bank's, and the bank is the only source of truth for whether
/// money actually moved.
pub async fn reconcile(pool: &PgPool, settlement_file: &[SettlementLine]) -> Result<ReconResult> {
    let mut result = ReconResult::default();

    // Our side: every settled refund with a gateway_ref.
    let settled: Vec<(Uuid, String, i64)> = sqlx::query_as(
        "SELECT id, gateway_ref, amount_cents FROM refunds \
         WHERE status='settled' AND gateway_ref IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    // Index the file by gateway_ref for matching.
    let mut file_by_ref: HashMap<&str, &SettlementLine> = HashMap::new();
    for line in settlement_file {
        file_by_ref.insert(line.gateway_ref.as_str(), line);
    }

    // Track which file lines we matched, to find unmatched lines (class two).
    let mut matched_refs: HashMap<&str, ()> = HashMap::new();

    for (refund_id, gateway_ref, db_amount) in &settled {
        match file_by_ref.get(gateway_ref.as_str()) {
            None => {
                // Class one: settled in DB, missing from the file.
                result.settled_missing_from_file.push(*refund_id);
            }
            Some(line) => {
                matched_refs.insert(gateway_ref.as_str(), ());
                if line.amount_cents != *db_amount {
                    // Class three: amount mismatch.
                    result.amount_mismatch.push(AmountMismatch {
                        gateway_ref: gateway_ref.clone(),
                        refund_id: *refund_id,
                        db_amount_cents: *db_amount,
                        file_amount_cents: line.amount_cents,
                    });
                }
            }
        }
    }

    // Class two: settlement lines with no matching settled refund row. Money
    // that left the account our system never recorded.
    for line in settlement_file {
        if !matched_refs.contains_key(line.gateway_ref.as_str()) {
            result.line_with_no_refund.push(line.gateway_ref.clone());
        }
    }

    Ok(result)
}
