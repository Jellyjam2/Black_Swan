use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use rand::RngCore;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

// Workspace contracts
use black_swan_security::{ActiveTrustGate, IngressTrustGate, WirePacket};
use black_swan_state::{LogCommand, PureState, StateMachineReducer};
use black_swan_storage::{SharedWAL, WalEntry};
use black_swan_transport::{SecureTransportEngine, TokioTransportEngine};

// ------------------------------------------------------------------
// SECURITY CAPABILITY CONSTANTS
// ------------------------------------------------------------------

const CAPABILITY_EXEC: &str = "compute.execute";

// ------------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------------

#[derive(Clone)]
struct PipelineConfig {
    listen_address: SocketAddr,
    max_concurrent_frames: usize,
    clock_skew_tolerance_secs: u64,
}

// ------------------------------------------------------------------
// CORE DAEMON
// ------------------------------------------------------------------

struct CoordinatorDaemon {
    config: PipelineConfig,
    transport: Arc<TokioTransportEngine>,
    trust_gate: Arc<ActiveTrustGate>,
    state_machine: Arc<Mutex<PureState>>,
    concurrency_gate: Arc<Semaphore>,
    wal: Arc<SharedWAL>,
}

impl CoordinatorDaemon {
    pub fn new(config: PipelineConfig) -> Self {
        let max_frames = config.max_concurrent_frames;

        let transport = Arc::new(TokioTransportEngine::new(config.listen_address));

        let trust_gate = Arc::new(ActiveTrustGate::new(config.clock_skew_tolerance_secs));

        let wal = Arc::new(SharedWAL::new("node_a"));

        Self {
            config,
            transport,
            trust_gate,
            state_machine: Arc::new(Mutex::new(PureState::default())),
            concurrency_gate: Arc::new(Semaphore::new(max_frames)),
            wal,
        }
    }

    pub async fn run_pipeline_kernel(&self) -> Result<()> {
        // ----------------------------------------------------------
        // WAL RECOVERY
        // ----------------------------------------------------------

        {
            println!("[KERNEL] WAL replay starting...");

            let wal_guard = self.wal.inner.read().await;

            let history = wal_guard.replay().unwrap_or_default();

            drop(wal_guard);

            let recovered_count = history.len();

            let mut state_guard = self.state_machine.lock().await;

            for entry in history {
                state_guard.apply(&entry.command);
            }

            println!("[KERNEL] WAL replay complete. entries={}", recovered_count);
        }

        // ----------------------------------------------------------
        // START NETWORK LISTENER
        // ----------------------------------------------------------

        println!("[KERNEL] Listening on {}", self.config.listen_address);

        self.transport.run_listener().await?;

        let transport_ref = self.transport.clone();
        let gate_ref = self.trust_gate.clone();
        let state_ref = self.state_machine.clone();
        let semaphore_ref = self.concurrency_gate.clone();
        let wal_ref = self.wal.clone();

        // ----------------------------------------------------------
        // INGEST LOOP
        // ----------------------------------------------------------

        tokio::spawn(async move {
            while let Some((peer_addr, wire_packet)) = transport_ref.recv_packet().await {
                let permit = match semaphore_ref.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let gate_clone = gate_ref.clone();
                let state_clone = state_ref.clone();
                let transport_clone = transport_ref.clone();
                let wal_clone = wal_ref.clone();

                tokio::spawn(async move {
                    let start = Instant::now();

                    let validation_result = gate_clone
                        .verify_and_authorize(&wire_packet, CAPABILITY_EXEC)
                        .await;

                    match validation_result {
                        Ok(validated) => {
                            let command_parse =
                                serde_json::from_slice::<LogCommand>(&validated.payload);

                            match command_parse {
                                Ok(cmd) => {
                                    println!(
                                        "[OK] validated packet latency={}µs",
                                        start.elapsed().as_micros()
                                    );

                                    // ----------------------------------
                                    // BUILD WAL ENTRY
                                    // ----------------------------------

                                    let entry = WalEntry {
                                        index: 0,
                                        term: 0,
                                        command: cmd.clone(),
                                    };

                                    // ----------------------------------
                                    // WRITE-AHEAD LOG FIRST
                                    // ----------------------------------

                                    {
                                        let wal_guard = wal_clone.inner.write().await;

                                        if let Err(e) = wal_guard.append(&entry) {
                                            eprintln!("[FATAL WAL ERROR] {}", e);

                                            std::process::exit(1);
                                        }
                                    }

                                    // ----------------------------------
                                    // APPLY STATE
                                    // ----------------------------------

                                    let mut state_guard = state_clone.lock().await;

                                    state_guard.apply(&cmd);
                                }

                                Err(parse_err) => {
                                    eprintln!("[INVALID COMMAND PAYLOAD] {:?}", parse_err);
                                }
                            }
                        }

                        Err(err) => {
                            eprintln!("[SECURITY DROP] peer={:?} reason={:?}", peer_addr, err);

                            transport_clone.close_connection(peer_addr).await;
                        }
                    }

                    drop(permit);
                });
            }
        });

        Ok(())
    }
}

// ------------------------------------------------------------------
// MAIN
// ------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    println!("--- BLACK SWAN PIPELINE v3 (WAL + CONSENSUS READY) ---");

    let server_addr: SocketAddr = "127.0.0.1:9199".parse()?;

    let config = PipelineConfig {
        listen_address: server_addr,
        max_concurrent_frames: 512,
        clock_skew_tolerance_secs: 30,
    };

    let daemon = CoordinatorDaemon::new(config);

    daemon.run_pipeline_kernel().await?;

    sleep(Duration::from_millis(50)).await;

    // --------------------------------------------------------------
    // TEST HARNESS
    // --------------------------------------------------------------

    let mut csprng = OsRng;

    let mut secret_key = [0u8; 32];

    csprng.fill_bytes(&mut secret_key);

    let signing_key = SigningKey::from_bytes(&secret_key);

    // --------------------------------------------------------------
    // REGISTER TRUST IDENTITY
    // --------------------------------------------------------------

    let public_key = signing_key.verifying_key();

    daemon
        .trust_gate
        .register_identity(
            "worker_shard_01".into(),
            public_key,
            vec![CAPABILITY_EXEC.into()],
        )
        .await;

    // --------------------------------------------------------------
    // CREATE COMMAND
    // --------------------------------------------------------------

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

    // --------------------------------------------------------------
    // SIGN PACKET
    // --------------------------------------------------------------

    let signature = signing_key.sign(&msg).to_bytes().to_vec();

    let packet = WirePacket {
        sender_id: "worker_shard_01".into(),
        nonce,
        timestamp,
        raw_payload: payload,
        signature,
    };

    // --------------------------------------------------------------
    // SEND PACKET
    // --------------------------------------------------------------

    let client = TokioTransportEngine::new("127.0.0.1:9200".parse()?);

    let route = client.open_connection(server_addr).await?;

    client.send_packet(route, packet).await?;

    sleep(Duration::from_millis(150)).await;

    // --------------------------------------------------------------
    // VERIFY STATE
    // --------------------------------------------------------------

    let state = daemon.state_machine.lock().await;

    let view = state.create_view();

    println!(
        "[STATE] active graphs = {}",
        view.snapshot.active_graphs.len()
    );

    Ok(())
}
