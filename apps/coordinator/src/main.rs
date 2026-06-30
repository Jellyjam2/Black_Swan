use std::net::SocketAddr;

use anyhow::Result;
use black_swan_coordinator::{CoordinatorDaemon, PipelineConfig};

#[tokio::main]
async fn main() -> Result<()> {
    println!("--- BLACK SWAN COORDINATOR DAEMON v3 ---");

    let server_addr: SocketAddr = "127.0.0.1:9199".parse()?;

    let config = PipelineConfig {
        listen_address: server_addr,
        max_concurrent_frames: 512,
        clock_skew_tolerance_secs: 30,
    };

    let daemon = CoordinatorDaemon::new(config);

    daemon.run_pipeline_kernel().await?;

    println!("[KERNEL] Ready. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;

    println!("[KERNEL] Shutdown requested.");

    Ok(())
}
