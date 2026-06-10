//! Refund engine: refunds as an explicit, auditable state machine.
//!
//! This crate implements the design from "Why refund processing is harder than
//! it looks" (<https://czeresniowski.dev/writing/why-refund-processing-is-harder-than-it-looks>):
//! row-locked over-refund prevention, idempotency keyed on the refund's own
//! uuid, order-tolerant deduped webhooks, the 12k-incident converge-don't-guess
//! recovery, separation-of-duties approvals, settlement reconciliation, and PAN
//! redaction.
//!
//! All database access uses runtime sqlx (`query`, `query_as`, `FromRow`); no
//! compile-time `query!` macros, so the crate compiles without a database.

// Money literals are grouped as dollars_cents on purpose, e.g. `100_00` reads
// as "100 dollars, 00 cents". That is intentional, not inconsistent.
#![allow(clippy::inconsistent_digit_grouping)]

pub mod approval;
pub mod error;
pub mod gateway;
pub mod model;
pub mod reconcile;
pub mod recover;
pub mod redact;
pub mod request;
pub mod submit;
pub mod webhook;

pub use error::{RefundError, Result};
pub use gateway::{Gateway, GatewayOutcome, GatewayRefund, MockConfig, MockGateway};
pub use model::{Charge, Refund, Status};

use sqlx::PgPool;

/// Embeds the SQL files under `migrations/` at compile time (no DB needed to
/// compile) and applies them at runtime. Examples and tests call this on
/// startup.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Apply all pending migrations. Idempotent.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR.run(pool).await.map_err(sqlx::Error::from)?;
    Ok(())
}

/// Connect to the database at `DATABASE_URL` and apply migrations. A small
/// convenience for examples and tests.
pub async fn connect_and_migrate(database_url: &str) -> Result<PgPool> {
    let pool = PgPool::connect(database_url).await?;
    migrate(&pool).await?;
    Ok(pool)
}

/// Truncate every table the engine owns, so examples and tests are idempotent
/// on re-run. Order respects foreign keys; `CASCADE` covers the rest.
pub async fn reset(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "TRUNCATE refund_transitions, webhook_events, refunds, charges RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}
