use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ed25519_dalek::{Signature, Verifier, VerifyingKey as PublicKey};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ==========================================================
// WIRE PACKETS
// ==========================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WirePacket {
    pub sender_id: String,
    pub nonce: u64,
    pub timestamp: u64,
    pub raw_payload: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ValidatedPacket {
    pub sender_id: String,
    pub payload: Vec<u8>,
}

// ==========================================================
// SECURITY ERRORS
// ==========================================================

#[derive(Debug, Clone, PartialEq)]
pub enum SecurityError {
    SignatureMismatch,
    ReplayDetected,
    ExpiredTimestamp,
    UnauthorizedCapability,
    UnknownIdentity,
}

// ==========================================================
// SHARD IDENTITY
// ==========================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardIdentity {
    pub public_key_bytes: Vec<u8>,
    pub allowed_capabilities: Vec<String>,
}

// ==========================================================
// REGISTRIES
// ==========================================================

type ReplayKey = (String, u64);

pub struct IdentityRegistry {
    pub storage: Mutex<HashMap<String, ShardIdentity>>,
}

pub struct NonceRegistry {
    pub storage: Mutex<HashSet<ReplayKey>>,
}

// ==========================================================
// TRUST GATE TRAIT
// ==========================================================

#[async_trait]
pub trait IngressTrustGate: Send + Sync {
    async fn verify_and_authorize(
        &self,
        packet: &WirePacket,
        required_cap: &str,
    ) -> Result<ValidatedPacket, SecurityError>;

    async fn register_identity(
        &self,
        identity_id: String,
        key: PublicKey,
        allowed_caps: Vec<String>,
    );
}

// ==========================================================
// ACTIVE TRUST GATE
// ==========================================================

pub struct ActiveTrustGate {
    pub identities: IdentityRegistry,
    pub seen_nonces: NonceRegistry,
    pub clock_skew_window_secs: u64,
}

impl ActiveTrustGate {
    pub fn new(clock_skew_window_secs: u64) -> Self {
        Self {
            identities: IdentityRegistry {
                storage: Mutex::new(HashMap::new()),
            },
            seen_nonces: NonceRegistry {
                storage: Mutex::new(HashSet::new()),
            },
            clock_skew_window_secs,
        }
    }
}

// ==========================================================
// IMPLEMENTATION
// ==========================================================

#[async_trait]
impl IngressTrustGate for ActiveTrustGate {
    async fn register_identity(
        &self,
        identity_id: String,
        key: PublicKey,
        allowed_caps: Vec<String>,
    ) {
        let mut guard = self.identities.storage.lock().await;

        let new_shard = ShardIdentity {
            public_key_bytes: key.to_bytes().to_vec(),
            allowed_capabilities: allowed_caps,
        };

        guard.insert(identity_id, new_shard);
    }

    async fn verify_and_authorize(
        &self,
        packet: &WirePacket,
        required_cap: &str,
    ) -> Result<ValidatedPacket, SecurityError> {
        let replay_key = (packet.sender_id.clone(), packet.nonce);

        // ======================================================
        // REPLAY CHECK
        // ======================================================

        {
            let nonce_guard = self.seen_nonces.storage.lock().await;

            if nonce_guard.contains(&replay_key) {
                return Err(SecurityError::ReplayDetected);
            }
        }

        // ======================================================
        // TIMESTAMP VALIDATION
        // ======================================================

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let drift_forward = packet.timestamp.saturating_sub(now);
        let drift_backward = now.saturating_sub(packet.timestamp);

        if drift_forward > self.clock_skew_window_secs
            || drift_backward > self.clock_skew_window_secs
        {
            return Err(SecurityError::ExpiredTimestamp);
        }

        // ======================================================
        // LOAD IDENTITY
        // ======================================================

        let shard = {
            let identities_guard = self.identities.storage.lock().await;

            identities_guard
                .get(&packet.sender_id)
                .cloned()
                .ok_or(SecurityError::UnknownIdentity)?
        };

        // ======================================================
        // CAPABILITY CHECK
        // ======================================================

        let authorized = shard
            .allowed_capabilities
            .iter()
            .any(|cap| cap == required_cap);

        if !authorized {
            return Err(SecurityError::UnauthorizedCapability);
        }

        // ======================================================
        // REBUILD SIGNED MESSAGE
        // ======================================================

        let mut msg_buffer = Vec::new();

        msg_buffer.extend_from_slice(packet.sender_id.as_bytes());
        msg_buffer.extend_from_slice(&packet.nonce.to_be_bytes());
        msg_buffer.extend_from_slice(&packet.timestamp.to_be_bytes());
        msg_buffer.extend_from_slice(&packet.raw_payload);

        // ======================================================
        // SIGNATURE PARSE
        // ======================================================

        let sig_bytes: [u8; 64] = packet
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| SecurityError::SignatureMismatch)?;

        let parsed_signature = Signature::from_bytes(&sig_bytes);

        // ======================================================
        // PUBLIC KEY PARSE
        // ======================================================

        let pk_bytes: [u8; 32] = shard
            .public_key_bytes
            .as_slice()
            .try_into()
            .map_err(|_| SecurityError::UnknownIdentity)?;

        let verified_public_key =
            PublicKey::from_bytes(&pk_bytes).map_err(|_| SecurityError::UnknownIdentity)?;

        // ======================================================
        // VERIFY SIGNATURE
        // ======================================================

        verified_public_key
            .verify(&msg_buffer, &parsed_signature)
            .map_err(|_| SecurityError::SignatureMismatch)?;

        // ======================================================
        // STORE NONCE
        // ======================================================

        {
            let mut nonce_write = self.seen_nonces.storage.lock().await;

            if !nonce_write.insert(replay_key) {
                return Err(SecurityError::ReplayDetected);
            }
        }

        Ok(ValidatedPacket {
            sender_id: packet.sender_id.clone(),
            payload: packet.raw_payload.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    const CAP_EXEC: &str = "compute.execute";

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn test_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    fn build_packet(
        signing_key: &SigningKey,
        sender_id: &str,
        nonce: u64,
        timestamp: u64,
        payload: Vec<u8>,
    ) -> WirePacket {
        let mut msg = Vec::new();

        msg.extend_from_slice(sender_id.as_bytes());
        msg.extend_from_slice(&nonce.to_be_bytes());
        msg.extend_from_slice(&timestamp.to_be_bytes());
        msg.extend_from_slice(&payload);

        let signature = signing_key.sign(&msg).to_bytes().to_vec();

        WirePacket {
            sender_id: sender_id.to_string(),
            nonce,
            timestamp,
            raw_payload: payload,
            signature,
        }
    }

    async fn registered_gate(allowed_caps: Vec<String>) -> (ActiveTrustGate, SigningKey) {
        let gate = ActiveTrustGate::new(30);
        let signing_key = test_signing_key();
        let public_key = signing_key.verifying_key();

        gate.register_identity("worker_shard_01".into(), public_key, allowed_caps)
            .await;

        (gate, signing_key)
    }

    #[tokio::test]
    async fn accepts_valid_packet_once() {
        let (gate, signing_key) = registered_gate(vec![CAP_EXEC.into()]).await;

        let packet = build_packet(
            &signing_key,
            "worker_shard_01",
            1,
            now_secs(),
            b"{\"ok\":true}".to_vec(),
        );

        let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn rejects_replayed_nonce() {
        let (gate, signing_key) = registered_gate(vec![CAP_EXEC.into()]).await;

        let packet = build_packet(
            &signing_key,
            "worker_shard_01",
            42,
            now_secs(),
            b"{\"ok\":true}".to_vec(),
        );

        let first = gate.verify_and_authorize(&packet, CAP_EXEC).await;
        assert!(first.is_ok());

        let second = gate.verify_and_authorize(&packet, CAP_EXEC).await;
        assert_eq!(second.unwrap_err(), SecurityError::ReplayDetected);
    }

    #[tokio::test]
    async fn rejects_expired_timestamp() {
        let (gate, signing_key) = registered_gate(vec![CAP_EXEC.into()]).await;

        let packet = build_packet(
            &signing_key,
            "worker_shard_01",
            2,
            now_secs() - 120,
            b"{\"ok\":true}".to_vec(),
        );

        let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

        assert_eq!(result.unwrap_err(), SecurityError::ExpiredTimestamp);
    }

    #[tokio::test]
    async fn rejects_invalid_signature() {
        let (gate, signing_key) = registered_gate(vec![CAP_EXEC.into()]).await;

        let mut packet = build_packet(
            &signing_key,
            "worker_shard_01",
            3,
            now_secs(),
            b"{\"ok\":true}".to_vec(),
        );

        packet.raw_payload = b"{\"tampered\":true}".to_vec();

        let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

        assert_eq!(result.unwrap_err(), SecurityError::SignatureMismatch);
    }

    #[tokio::test]
    async fn rejects_unauthorized_capability() {
        let (gate, signing_key) = registered_gate(vec!["storage.read".into()]).await;

        let packet = build_packet(
            &signing_key,
            "worker_shard_01",
            4,
            now_secs(),
            b"{\"ok\":true}".to_vec(),
        );

        let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

        assert_eq!(result.unwrap_err(), SecurityError::UnauthorizedCapability);
    }
}
