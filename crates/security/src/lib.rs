use std::collections::HashMap;
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

pub struct IdentityRegistry {
    pub storage: Mutex<String>,
}

pub struct NonceRegistry {
    pub storage: Mutex<String>,
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
                storage: Mutex::new(String::from("{}")),
            },
            seen_nonces: NonceRegistry {
                storage: Mutex::new(String::from(" ")),
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

        let mut current_map: HashMap<String, ShardIdentity> =
            serde_json::from_str(&guard).unwrap_or_default();

        let new_shard = ShardIdentity {
            public_key_bytes: key.to_bytes().to_vec(),
            allowed_capabilities: allowed_caps,
        };

        current_map.insert(identity_id, new_shard);

        *guard = serde_json::to_string(&current_map).unwrap_or_default();
    }

    async fn verify_and_authorize(
        &self,
        packet: &WirePacket,
        required_cap: &str,
    ) -> Result<ValidatedPacket, SecurityError> {
        // ======================================================
        // REPLAY CHECK
        // ======================================================

        {
            let nonce_guard = self.seen_nonces.storage.lock().await;

            let lookup_token =
                format!(" {}-{} ", packet.sender_id, packet.nonce);

            if nonce_guard.contains(&lookup_token) {
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
        // LOAD IDENTITIES
        // ======================================================

        let identities_guard = self.identities.storage.lock().await;

        let current_map: HashMap<String, ShardIdentity> =
            serde_json::from_str(&identities_guard).unwrap_or_default();

        let shard = current_map
            .get(&packet.sender_id)
            .ok_or(SecurityError::UnknownIdentity)?;

        // ======================================================
        // CAPABILITY CHECK
        // ======================================================

        let mut authorized = false;

        for cap in &shard.allowed_capabilities {
            if cap == required_cap {
                authorized = true;
                break;
            }
        }

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
            PublicKey::from_bytes(&pk_bytes)
                .map_err(|_| SecurityError::UnknownIdentity)?;

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

            let lookup_token =
                format!(" {}-{} ", packet.sender_id, packet.nonce);

            nonce_write.push_str(&lookup_token);
        }

        Ok(ValidatedPacket {
            sender_id: packet.sender_id.clone(),
            payload: packet.raw_payload.clone(),
        })
    }
}