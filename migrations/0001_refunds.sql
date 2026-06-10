-- Schema for the refund engine. Amounts are integer minor units (cents)
-- stored as bigint, never floats. The refund's currency must match the
-- charge's. These tables are the system of record for refund intent and
-- the append-only audit trail that proves which half of a half-happened
-- refund actually moved money.

CREATE TABLE IF NOT EXISTS charges (
    id              uuid PRIMARY KEY,
    amount_captured bigint NOT NULL CHECK (amount_captured >= 0),
    currency        text   NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS refunds (
    id              uuid PRIMARY KEY,
    charge_id       uuid NOT NULL REFERENCES charges(id),
    amount_cents    bigint NOT NULL CHECK (amount_cents > 0),
    currency        text   NOT NULL,
    idempotency_key text   NOT NULL UNIQUE,
    gateway_ref     text,                -- null until submitted
    status          text   NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested','pending_review','submitted',
                          'settled','failed','canceled')),
    reason_code     text,                -- enumerated, not free text
    requested_by    text   NOT NULL,     -- actor; feeds separation of duties
    failure_reason  text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS refunds_charge_id_idx ON refunds (charge_id);
CREATE UNIQUE INDEX IF NOT EXISTS refunds_gateway_ref_idx ON refunds (gateway_ref)
    WHERE gateway_ref IS NOT NULL;

-- Append-only. Never UPDATE or DELETE here. This is what a dispute,
-- an auditor, or a 3am on-call query reads to reconstruct truth.
CREATE TABLE IF NOT EXISTS refund_transitions (
    id          bigserial PRIMARY KEY,
    refund_id   uuid NOT NULL REFERENCES refunds(id),
    from_status text,
    to_status   text NOT NULL,
    actor       text NOT NULL,          -- 'user:1234', 'job:...', 'webhook'
    reason      text,
    at          timestamptz NOT NULL DEFAULT now()
);

-- Webhook deduplication. The gateway delivers events out of order and
-- duplicated; inserting an event id we already saw is a no-op.
CREATE TABLE IF NOT EXISTS webhook_events (
    id          text PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now()
);
