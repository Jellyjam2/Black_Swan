use anyhow::Result;
use black_swan_coordinator::{CoordinatorDaemon, PipelineConfig};

#[tokio::main]
async fn main() -> Result<()> {
    println!("--- BLACK SWAN COORDINATOR DAEMON v3 ---");

    let config = PipelineConfig::from_env()?;

    let daemon = CoordinatorDaemon::new(config);

    daemon.run_pipeline_kernel().await?;

    println!("[KERNEL] Ready. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;

    println!("[KERNEL] Shutdown requested.");

    Ok(())
}
