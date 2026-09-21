//! ACP protocol-version negotiation.
//!
//! One pi-acp binary serves both ACP v1 and v2. The SDK's
//! [`AgentProtocolRouter`] selects one of two **native** implementations from
//! the client's `initialize`; from then on the SDK is a raw-frame pass-through,
//! so each implementation builds and parses its own protocol's frames and no
//! frame is ever converted between the two.
//!
//! This module holds only the negotiated version and the per-connection record
//! of it. The frame builders live in [`crate::render`] (outbound) and the
//! native v2 handlers in [`crate::v2`].
//!
//! [`AgentProtocolRouter`]: agent_client_protocol::AgentProtocolRouter

use std::sync::atomic::{AtomicU8, Ordering};

/// The ACP wire protocol version a single connection negotiated.
///
/// A connection negotiates exactly once, in `initialize`, so this is fixed for
/// the lifetime of a connection (see [`NegotiatedProtocol`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// ACP protocol version 1 — the stable default.
    V1,
    /// ACP protocol version 2 — the unstable draft.
    V2,
}

impl Protocol {
    /// The numeric wire version.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Inverse of [`Protocol::as_u8`]; anything that is not 2 is v1.
    #[must_use]
    pub const fn from_u8(value: u8) -> Self {
        if value == 2 {
            Self::V2
        } else {
            Self::V1
        }
    }

    /// Whether this connection speaks the v2 draft.
    #[must_use]
    pub const fn is_v2(self) -> bool {
        matches!(self, Self::V2)
    }
}

/// The protocol version negotiated on an agent's connection.
///
/// `AcpAgent` is a per-connection object (the binary creates one and calls
/// `run_with` once), so a plain atomic is enough: the router hands the whole
/// connection to exactly one implementation, and that implementation's
/// `initialize` handler records the version it answered with.
#[derive(Debug, Default)]
pub struct NegotiatedProtocol(AtomicU8);

impl NegotiatedProtocol {
    /// A fresh record, defaulting to v1 until `initialize` says otherwise.
    #[must_use]
    pub fn new() -> Self {
        Self(AtomicU8::new(Protocol::V1.as_u8()))
    }

    /// The negotiated version.
    #[must_use]
    pub fn get(&self) -> Protocol {
        Protocol::from_u8(self.0.load(Ordering::Relaxed))
    }

    /// Record the version this connection answered `initialize` with.
    pub fn set(&self, protocol: Protocol) {
        self.0.store(protocol.as_u8(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_round_trips_through_u8() {
        assert_eq!(Protocol::V1.as_u8(), 1);
        assert_eq!(Protocol::V2.as_u8(), 2);
        assert_eq!(Protocol::from_u8(1), Protocol::V1);
        assert_eq!(Protocol::from_u8(2), Protocol::V2);
        // Anything else is not a version pi-acp knows; v1 is the safe default.
        assert_eq!(Protocol::from_u8(0), Protocol::V1);
        assert_eq!(Protocol::from_u8(9), Protocol::V1);
        assert!(!Protocol::V1.is_v2());
        assert!(Protocol::V2.is_v2());
    }

    #[test]
    fn negotiated_protocol_defaults_to_v1_and_records_changes() {
        let negotiated = NegotiatedProtocol::new();
        assert_eq!(negotiated.get(), Protocol::V1);
        negotiated.set(Protocol::V2);
        assert_eq!(negotiated.get(), Protocol::V2);
        negotiated.set(Protocol::V1);
        assert_eq!(negotiated.get(), Protocol::V1);
    }
}
