use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::PgPool;

use crate::error::{RefundError, Result};

type HmacSha256 = Hmac<Sha256>;

/// A webhook event from the gateway. Settlement comes over these, and they are
/// the most adversarial input in the system: out of order, duplicated, and
/// occasionally a `refund.failed` for a refund you were sure went through.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: WebhookData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookData {
    pub gateway_ref: String,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Outcome of handling a webhook, so callers/tests can assert what happened
/// without re-querying.
#[derive(Debug, PartialEq, Eq)]
pub enum WebhookResult {
    /// First time we saw this event; it was applied (rows may or may not have
    /// changed depending on the guards).
    Applied,
    /// We had already processed this event id; dropped as a no-op.
    Duplicate,
}

/// Sign a raw body with the shared secret, producing the hex signature the
/// gateway would send. Used by tests and by anyone simulating the gateway.
pub fn sign(raw_body: &[u8], secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(raw_body);
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time verify of a hex signature against the raw body.
pub fn verify_signature(raw_body: &[u8], sig: &str, secret: &[u8]) -> bool {
    let expected = match hex::decode(sig) {
        Ok(b) => b,
        Err(_) => return false,
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(raw_body);
    mac.verify_slice(&expected).is_ok()
}

/// Handle a gateway webhook. Verify the signature on every payload, dedupe on
/// the event id so replays are no-ops, and tolerate any order by keying off
/// the refund's `gateway_ref` rather than assuming a sequence.
///
/// This is the article's `handle_webhook` ported verbatim, with the
/// transition rows the article notes it elided for brevity but calls "not
/// optional in the real handler".
pub async fn handle_webhook(
    pool: &PgPool,
    event: &WebhookEvent,
    raw_body: &[u8],
    sig: &str,
    secret: &[u8],
) -> Result<WebhookResult> {
    if !verify_signature(raw_body, sig, secret) {
        return Err(RefundError::BadSignature);
    }

    // Dedupe: dropping a row we already processed is a no-op. INSERT ... ON
    // CONFLICT DO NOTHING RETURNING id returns no row on a duplicate.
    let inserted: Option<(String,)> = sqlx::query_as(
        "INSERT INTO webhook_events (id) VALUES ($1) \
         ON CONFLICT (id) DO NOTHING RETURNING id",
    )
    .bind(&event.id)
    .fetch_optional(pool)
    .await?;

    if inserted.is_none() {
        return Ok(WebhookResult::Duplicate); // duplicate delivery
    }

    let ref_ = &event.data.gateway_ref;

    // The state change and its audit row go in the SAME transaction, so there
    // is no window where a refund advanced but the log missed it.
    let mut tx = pool.begin().await?;

    match event.event_type.as_str() {
        "refund.settled" => {
            // Out-of-order safe: only advance from a non-terminal state. A
            // settled for an already-settled row, or for a row since marked
            // failed, changes nothing rather than resurrecting a dead refund.
            // RETURNING the rows lets us write the transition with an accurate
            // pre-update `from_status` (it comes from the matched WHERE clause).
            apply_settled(&mut tx, ref_).await?;
        }
        "refund.failed" => {
            apply_failed(&mut tx, ref_, event.data.reason.as_deref()).await?;
        }
        _ => {
            // Unknown event types are accepted (deduped) but apply no state
            // change. The gateway may add types we don't model.
        }
    }

    tx.commit().await?;
    Ok(WebhookResult::Applied)
}

/// Advance every row at `gateway_ref` from a non-terminal state to `settled`,
/// and append the audit row in the same transaction with the correct
/// pre-update `from_status`. A single `WITH ... UPDATE ... RETURNING` captures
/// the old status atomically, so the transition can never disagree with the
/// state change.
async fn apply_settled(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    gateway_ref: &str,
) -> Result<()> {
    sqlx::query(
        "WITH moved AS ( \
            UPDATE refunds SET status='settled', updated_at=now() \
             WHERE gateway_ref=$1 AND status IN ('submitted','requested') \
             RETURNING id, 'settled'::text AS to_status \
         ) \
         INSERT INTO refund_transitions (refund_id, from_status, to_status, actor, reason) \
         SELECT moved.id, \
                (SELECT to_status FROM refund_transitions t \
                  WHERE t.refund_id = moved.id ORDER BY t.id DESC LIMIT 1), \
                moved.to_status, 'webhook', NULL \
           FROM moved",
    )
    .bind(gateway_ref)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Advance every row at `gateway_ref` that is not already terminal to
/// `failed`, recording `failure_reason`, and append the audit row in the same
/// transaction. Mirrors `apply_settled`.
async fn apply_failed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    gateway_ref: &str,
    reason: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "WITH moved AS ( \
            UPDATE refunds SET status='failed', failure_reason=$2, updated_at=now() \
             WHERE gateway_ref=$1 AND status NOT IN ('settled','canceled') \
             RETURNING id, 'failed'::text AS to_status \
         ) \
         INSERT INTO refund_transitions (refund_id, from_status, to_status, actor, reason) \
         SELECT moved.id, \
                (SELECT to_status FROM refund_transitions t \
                  WHERE t.refund_id = moved.id ORDER BY t.id DESC LIMIT 1), \
                moved.to_status, 'webhook', $2 \
           FROM moved",
    )
    .bind(gateway_ref)
    .bind(reason)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
