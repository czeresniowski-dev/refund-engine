use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// What the gateway returns for a refund. `id` is the `gateway_ref` we
/// persist. The gateway dedupes on the idempotency key: a second
/// `create_refund` with the same key returns the SAME object without moving
/// money again. That property is the entire reason a crash-and-retry never
/// double-pays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayRefund {
    pub id: String,
    pub amount: i64,
    pub currency: String,
    pub idempotency_key: String,
}

/// Outcome of a gateway call. `Accepted` carries the (possibly deduped)
/// refund. `Dropped` models the crash window: the gateway accepted and
/// recorded the refund, but the response never reached us (process SIGKILLed,
/// connection reset). `TimedOut` is the same uncertainty without acceptance
/// being guaranteed.
#[derive(Debug)]
pub enum GatewayOutcome {
    Accepted(GatewayRefund),
    Dropped,
    TimedOut,
}

/// The gateway interface the engine talks to. Kept as a trait so the demos
/// can inject a mock; in production this wraps an HTTP client to a
/// Stripe-class provider.
pub trait Gateway: Send + Sync {
    /// Create (or return the deduped original of) a refund. The
    /// `idempotency_key` is the refund row's own uuid id.
    fn create_refund(
        &self,
        amount: i64,
        currency: &str,
        idempotency_key: &str,
    ) -> GatewayOutcome;

    /// Look a refund up by idempotency key without creating one. This is the
    /// converge-don't-guess question recovery asks for every `submitted` row:
    /// "do you already hold a refund with this key?"
    fn lookup_by_key(&self, idempotency_key: &str) -> Option<GatewayRefund>;
}

/// Behaviour knobs for the mock so a demo can reproduce the incident.
#[derive(Debug, Clone, Copy, Default)]
pub struct MockConfig {
    /// Accept and record the refund on the gateway side, but DROP the
    /// response so the caller never learns the assigned `gateway_ref`. This
    /// is the SIGKILL-in-the-window scenario from the 12k incident.
    pub drop_response: bool,
    /// Time out without recording anything (the call genuinely did not land).
    pub time_out: bool,
}

/// In-memory gateway that dedupes on idempotency key. The internal map is the
/// gateway's own system of record; `issued_count` counts how many DISTINCT
/// refunds it actually created, which is what the incident demo asserts never
/// exceeds the number of logical refunds.
pub struct MockGateway {
    /// idempotency_key -> the one refund object issued for it.
    store: Mutex<HashMap<String, GatewayRefund>>,
    /// Monotonic counter for assigning gateway refs.
    seq: Mutex<u64>,
    /// How the next call behaves. Wrapped in a Mutex so a demo can flip it
    /// partway through a batch.
    config: Mutex<MockConfig>,
    /// Number of distinct refunds the gateway has actually issued.
    issued: Mutex<u64>,
}

impl MockGateway {
    pub fn new() -> Self {
        MockGateway {
            store: Mutex::new(HashMap::new()),
            seq: Mutex::new(0),
            config: Mutex::new(MockConfig::default()),
            issued: Mutex::new(0),
        }
    }

    pub fn with_config(config: MockConfig) -> Self {
        let g = MockGateway::new();
        *g.config.lock().unwrap() = config;
        g
    }

    /// Reconfigure behaviour mid-flight (used to start dropping responses
    /// partway through a batch to simulate the SIGKILL).
    pub fn set_config(&self, config: MockConfig) {
        *self.config.lock().unwrap() = config;
    }

    /// Distinct refunds the gateway actually created. Equals the number of
    /// logical refunds when there are zero double-refunds.
    pub fn issued_count(&self) -> u64 {
        *self.issued.lock().unwrap()
    }

    fn next_ref(&self) -> String {
        let mut s = self.seq.lock().unwrap();
        *s += 1;
        format!("re_mock_{:08}", *s)
    }
}

impl Default for MockGateway {
    fn default() -> Self {
        MockGateway::new()
    }
}

impl Gateway for MockGateway {
    fn create_refund(
        &self,
        amount: i64,
        currency: &str,
        idempotency_key: &str,
    ) -> GatewayOutcome {
        let config = *self.config.lock().unwrap();

        // Dedupe first: if we already hold this key, return the original and
        // never issue a second refund, regardless of config. This is the
        // property the whole design leans on.
        {
            let store = self.store.lock().unwrap();
            if let Some(existing) = store.get(idempotency_key) {
                let existing = existing.clone();
                // Even a deduped hit can have its response dropped on the way
                // back, but no new money moved.
                return if config.drop_response {
                    GatewayOutcome::Dropped
                } else {
                    GatewayOutcome::Accepted(existing)
                };
            }
        }

        if config.time_out {
            // Nothing recorded; the call did not land. A retry with the same
            // key is safe by construction.
            return GatewayOutcome::TimedOut;
        }

        // New refund: record it (money moves once) and bump the issued count.
        let refund = GatewayRefund {
            id: self.next_ref(),
            amount,
            currency: currency.to_string(),
            idempotency_key: idempotency_key.to_string(),
        };
        {
            let mut store = self.store.lock().unwrap();
            store.insert(idempotency_key.to_string(), refund.clone());
        }
        {
            let mut issued = self.issued.lock().unwrap();
            *issued += 1;
        }

        if config.drop_response {
            // The gateway accepted and recorded the refund, but the caller
            // never sees the ref. From the caller's side this looks like a
            // crash window: a 'submitted' row with no gateway_ref.
            GatewayOutcome::Dropped
        } else {
            GatewayOutcome::Accepted(refund)
        }
    }

    fn lookup_by_key(&self, idempotency_key: &str) -> Option<GatewayRefund> {
        self.store.lock().unwrap().get(idempotency_key).cloned()
    }
}
