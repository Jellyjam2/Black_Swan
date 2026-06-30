use std::time::{SystemTime, UNIX_EPOCH};

use black_swan_security::{ActiveTrustGate, IngressTrustGate, SecurityError, WirePacket};
use ed25519_dalek::{Signer, SigningKey};

const SENDER_ID: &str = "worker_shard_01";
const CAP_EXEC: &str = "compute.execute";

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn signed_packet(
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
    let key = signing_key(11);

    gate.register_identity(SENDER_ID.into(), key.verifying_key(), allowed_caps)
        .await;

    (gate, key)
}

#[tokio::test]
async fn trust_gate_accepts_valid_signed_packet() {
    let (gate, key) = registered_gate(vec![CAP_EXEC.into()]).await;

    let packet = signed_packet(&key, SENDER_ID, 1001, now_secs(), b"{\"ok\":true}".to_vec());

    let validated = gate.verify_and_authorize(&packet, CAP_EXEC).await.unwrap();

    assert_eq!(validated.sender_id, SENDER_ID);
    assert_eq!(validated.payload, b"{\"ok\":true}".to_vec());
}

#[tokio::test]
async fn trust_gate_rejects_replayed_nonce() {
    let (gate, key) = registered_gate(vec![CAP_EXEC.into()]).await;

    let packet = signed_packet(&key, SENDER_ID, 1002, now_secs(), b"{\"ok\":true}".to_vec());

    gate.verify_and_authorize(&packet, CAP_EXEC).await.unwrap();

    let replay = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(replay.unwrap_err(), SecurityError::ReplayDetected);
}

#[tokio::test]
async fn trust_gate_rejects_expired_timestamp() {
    let (gate, key) = registered_gate(vec![CAP_EXEC.into()]).await;

    let packet = signed_packet(
        &key,
        SENDER_ID,
        1003,
        now_secs() - 120,
        b"{\"ok\":true}".to_vec(),
    );

    let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(result.unwrap_err(), SecurityError::ExpiredTimestamp);
}

#[tokio::test]
async fn trust_gate_rejects_future_timestamp_outside_window() {
    let (gate, key) = registered_gate(vec![CAP_EXEC.into()]).await;

    let packet = signed_packet(
        &key,
        SENDER_ID,
        1004,
        now_secs() + 120,
        b"{\"ok\":true}".to_vec(),
    );

    let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(result.unwrap_err(), SecurityError::ExpiredTimestamp);
}

#[tokio::test]
async fn trust_gate_rejects_invalid_signature() {
    let (gate, key) = registered_gate(vec![CAP_EXEC.into()]).await;

    let mut packet = signed_packet(&key, SENDER_ID, 1005, now_secs(), b"{\"ok\":true}".to_vec());

    packet.raw_payload = b"{\"tampered\":true}".to_vec();

    let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(result.unwrap_err(), SecurityError::SignatureMismatch);
}

#[tokio::test]
async fn trust_gate_rejects_unauthorized_capability() {
    let (gate, key) = registered_gate(vec!["storage.read".into()]).await;

    let packet = signed_packet(&key, SENDER_ID, 1006, now_secs(), b"{\"ok\":true}".to_vec());

    let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(result.unwrap_err(), SecurityError::UnauthorizedCapability);
}

#[tokio::test]
async fn trust_gate_rejects_unknown_identity() {
    let gate = ActiveTrustGate::new(30);
    let key = signing_key(13);

    let packet = signed_packet(
        &key,
        "unregistered_worker",
        1007,
        now_secs(),
        b"{\"ok\":true}".to_vec(),
    );

    let result = gate.verify_and_authorize(&packet, CAP_EXEC).await;

    assert_eq!(result.unwrap_err(), SecurityError::UnknownIdentity);
}
