use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use rand::RngCore;
use tokio::time::sleep;

use black_swan_coordinator::{CoordinatorDaemon, PipelineConfig, CAPABILITY_EXEC};
use black_swan_security::{IngressTrustGate, WirePacket};
use black_swan_state::{LogCommand, StateMachineReducer};
use black_swan_transport::{SecureTransportEngine, TokioTransportEngine};

#[tokio::main]
async fn main() -> Result<()> {
    println!("--- BLACK SWAN LOCAL SIGNED PACKET DEMO ---");

    let server_addr: SocketAddr = "127.0.0.1:9199".parse()?;

    let config = PipelineConfig {
        listen_address: server_addr,
        max_concurrent_frames: 512,
        clock_skew_tolerance_secs: 30,
        current_term: 1,
    };

    let daemon = CoordinatorDaemon::new(config);

    daemon.run_pipeline_kernel().await?;

    sleep(Duration::from_millis(50)).await;

    let mut csprng = OsRng;

    let mut secret_key = [0u8; 32];
    csprng.fill_bytes(&mut secret_key);

    let signing_key = SigningKey::from_bytes(&secret_key);
    let public_key = signing_key.verifying_key();

    daemon
        .trust_gate
        .register_identity(
            "worker_shard_01".into(),
            public_key,
            vec![CAPABILITY_EXEC.into()],
        )
        .await;

    let mock_cmd = LogCommand::SubmitGraph {
        graph_id: "tx_integrated_graph_001".into(),
        payload: "{\"nodes\":[]}".into(),
    };

    let payload = serde_json::to_vec(&mock_cmd)?;

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let nonce = 42u64;

    let mut msg = Vec::new();
    msg.extend_from_slice(b"worker_shard_01");
    msg.extend_from_slice(&nonce.to_be_bytes());
    msg.extend_from_slice(&timestamp.to_be_bytes());
    msg.extend_from_slice(&payload);

    let signature = signing_key.sign(&msg).to_bytes().to_vec();

    let packet = WirePacket {
        sender_id: "worker_shard_01".into(),
        nonce,
        timestamp,
        raw_payload: payload,
        signature,
    };

    let client = TokioTransportEngine::new("127.0.0.1:9200".parse()?);

    let route = client.open_connection(server_addr).await?;

    client.send_packet(route, packet).await?;

    sleep(Duration::from_millis(150)).await;

    let state = daemon.state_machine.lock().await;
    let view = state.create_view();

    println!(
        "[STATE] active graphs = {}",
        view.snapshot.active_graphs.len()
    );
    println!("[DEMO] Complete.");

    Ok(())
}
