use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::{Mutex, Semaphore};

use black_swan_security::{ActiveTrustGate, IngressTrustGate};
use black_swan_state::{LogCommand, PureState, StateMachineReducer};
use black_swan_storage::{SharedWAL, WalEntry};
use black_swan_transport::{SecureTransportEngine, TokioTransportEngine};

pub const CAPABILITY_EXEC: &str = "compute.execute";

#[derive(Clone)]
pub struct PipelineConfig {
    pub listen_address: SocketAddr,
    pub max_concurrent_frames: usize,
    pub clock_skew_tolerance_secs: u64,
}

pub struct CoordinatorDaemon {
    pub config: PipelineConfig,
    pub transport: Arc<TokioTransportEngine>,
    pub trust_gate: Arc<ActiveTrustGate>,
    pub state_machine: Arc<Mutex<PureState>>,
    pub concurrency_gate: Arc<Semaphore>,
    pub wal: Arc<SharedWAL>,
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
        println!("[KERNEL] WAL replay starting...");

        {
            let wal_guard = self.wal.inner.read().await;
            let history = wal_guard.replay().unwrap_or_default();
            drop(wal_guard);

            let recovered_count = history.len();
            let mut state_guard = self.state_machine.lock().await;

            for entry in history {
                state_guard.apply(&entry.command);
            }

            println!("[KERNEL] WAL replay complete. entries={recovered_count}");
        }

        println!("[KERNEL] Listening on {}", self.config.listen_address);

        self.transport.run_listener().await?;

        let transport_ref = self.transport.clone();
        let gate_ref = self.trust_gate.clone();
        let state_ref = self.state_machine.clone();
        let semaphore_ref = self.concurrency_gate.clone();
        let wal_ref = self.wal.clone();

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

                                    let entry = WalEntry {
                                        index: 0,
                                        term: 0,
                                        command: cmd.clone(),
                                    };

                                    {
                                        let wal_guard = wal_clone.inner.write().await;

                                        if let Err(e) = wal_guard.append(&entry) {
                                            eprintln!("[FATAL WAL ERROR] {e}");
                                            std::process::exit(1);
                                        }
                                    }

                                    let mut state_guard = state_clone.lock().await;
                                    state_guard.apply(&cmd);
                                }

                                Err(parse_err) => {
                                    eprintln!("[INVALID COMMAND PAYLOAD] {parse_err:?}");
                                }
                            }
                        }

                        Err(err) => {
                            eprintln!("[SECURITY DROP] peer={peer_addr:?} reason={err:?}");

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
