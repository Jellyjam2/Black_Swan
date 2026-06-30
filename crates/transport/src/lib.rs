use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use black_swan_security::WirePacket;

// ------------------------------------------------------------------
// 1. TYPING DEFINITIONS & PROTOCOL ERRORS
// ------------------------------------------------------------------
pub type ConnectionId = SocketAddr;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum NetworkMessage {
    Payload(WirePacket),
    Ping,
    Pong,
}

#[derive(Debug)]
pub enum TransportError {
    ConnectionFailed(SocketAddr),
    WriteTimeout(ConnectionId),
    ChannelExhausted,
}

// ------------------------------------------------------------------
// 2. THE TRANSPORT CORE ENGINE TRAIT
// ------------------------------------------------------------------
#[async_trait]
pub trait SecureTransportEngine: Send + Sync {
    async fn open_connection(&self, target: SocketAddr) -> Result<ConnectionId>;
    async fn send_packet(&self, target: ConnectionId, packet: WirePacket) -> Result<()>;
    async fn recv_packet(&self) -> Option<(ConnectionId, WirePacket)>;
    async fn close_connection(&self, target: ConnectionId);
}

// ------------------------------------------------------------------
// 3. CORE RUNTIME ENGINE IMPLEMENTATION
// ------------------------------------------------------------------
pub struct TokioTransportEngine {
    listen_addr: SocketAddr,
    connections: Arc<RwLock<HashMap<ConnectionId, mpsc::Sender<NetworkMessage>>>>,
    ingress_tx: mpsc::Sender<(ConnectionId, WirePacket)>,
    ingress_rx: Arc<Mutex<mpsc::Receiver<(ConnectionId, WirePacket)>>>,
}

// Fallback Mutex wrapped to meet sync traits cleanly across threads
use tokio::sync::Mutex;

impl TokioTransportEngine {
    pub fn new(listen_addr: SocketAddr) -> Self {
        let (ingress_tx, ingress_rx) = mpsc::channel(1024);
        Self {
            listen_addr,
            connections: Arc::new(RwLock::new(HashMap::new())),
            ingress_tx,
            ingress_rx: Arc::new(Mutex::new(ingress_rx)),
        }
    }

    /// Spawns the TCP background driver to listen for incoming stream handshakes
    pub async fn run_listener(&self) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let connections_ref = self.connections.clone();
        let ingress_tx_ref = self.ingress_tx.clone();

        tokio::spawn(async move {
            while let Ok((stream, peer_addr)) = listener.accept().await {
                let (tx, rx) = mpsc::channel::<NetworkMessage>(256);

                // Mount tracking map immediately to handle duplex events
                connections_ref.write().await.insert(peer_addr, tx);

                let ingress_clone = ingress_tx_ref.clone();
                let connections_clone = connections_ref.clone();

                tokio::spawn(async move {
                    if let Err(e) =
                        Self::handle_socket_lifecycle(stream, peer_addr, rx, ingress_clone).await
                    {
                        eprintln!(
                            "[TRANSPORT WARNING] Connection closed with {} due to: {:?}",
                            peer_addr, e
                        );
                    }
                    connections_clone.write().await.remove(&peer_addr);
                });
            }
        });
        Ok(())
    }

    /// Drives frame processing over a single connection
    async fn handle_socket_lifecycle(
        stream: TcpStream,
        peer_addr: SocketAddr,
        mut outbound_rx: mpsc::Receiver<NetworkMessage>,
        ingress_tx: mpsc::Sender<(ConnectionId, WirePacket)>,
    ) -> Result<()> {
        // Enforce explicit length-delimited wire framing (prevents packet fragmentation corruption)
        let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

        loop {
            tokio::select! {
                // Handle outbound queue writes to the physical socket
                Some(msg) = outbound_rx.recv() => {
                    let raw_bytes = serde_json::to_vec(&msg)?;
                    framed.send(Bytes::from(raw_bytes)).await?;
                }

                // Handle inbound queue reads from the physical socket
                res = framed.next() => {
                    match res {
                        Some(Ok(bytes)) => {
                            let msg: NetworkMessage = serde_json::from_slice(&bytes)?;
                            match msg {
                                NetworkMessage::Payload(packet) => {
                                    if ingress_tx.send((peer_addr, packet)).await.is_err() { break; }
                                }
                                NetworkMessage::Ping => {
                                    let raw_pong = serde_json::to_vec(&NetworkMessage::Pong)?;
                                    framed.send(Bytes::from(raw_pong)).await?;
                                }
                                NetworkMessage::Pong => {}
                            }
                        }
                        _ => break, // Connection terminated by remote peer
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SecureTransportEngine for TokioTransportEngine {
    async fn open_connection(&self, target: SocketAddr) -> Result<ConnectionId> {
        if self.connections.read().await.contains_key(&target) {
            return Ok(target);
        }

        // Connect with exponential backoff strategy
        let mut attempts = 0;
        let mut delay = Duration::from_millis(50);
        let stream = loop {
            match TcpStream::connect(target).await {
                Ok(s) => break s,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        return Err(anyhow!(
                            "TRANSPORT_ERR: Target unreached after retries: {}",
                            e
                        ));
                    }
                    sleep(delay).await;
                    delay *= 2;
                }
            }
        };

        let (tx, rx) = mpsc::channel::<NetworkMessage>(256);
        self.connections.write().await.insert(target, tx);

        let ingress_tx_clone = self.ingress_tx.clone();
        let connections_clone = self.connections.clone();

        tokio::spawn(async move {
            let _ = Self::handle_socket_lifecycle(stream, target, rx, ingress_tx_clone).await;
            connections_clone.write().await.remove(&target);
        });

        Ok(target)
    }

    async fn send_packet(&self, target: ConnectionId, packet: WirePacket) -> Result<()> {
        let guard = self.connections.read().await;
        if let Some(tx) = guard.get(&target) {
            tx.send(NetworkMessage::Payload(packet))
                .await
                .map_err(|_| anyhow!("TRANSPORT_ERR: Channel write failed"))?;
            Ok(())
        } else {
            Err(anyhow!(
                "TRANSPORT_ERR: Active connection route non-existent"
            ))
        }
    }

    async fn recv_packet(&self) -> Option<(ConnectionId, WirePacket)> {
        let mut rx = self.ingress_rx.lock().await;
        rx.recv().await
    }

    async fn close_connection(&self, target: ConnectionId) {
        self.connections.write().await.remove(&target);
    }
}
