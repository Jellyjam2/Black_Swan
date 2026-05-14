use std::fs::{create_dir_all, OpenOptions};
use std::io::{Write, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub use black_swan_state::LogCommand;

// ======================================================
// WAL ENTRY FORMAT (APPEND-ONLY LOG)
// ======================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    pub index: usize,
    pub term: u64,
    pub command: LogCommand,
}

// ======================================================
// DISK WAL STORAGE ENGINE
// ======================================================

pub struct DiskWAL {
    file_path: PathBuf,
}

impl DiskWAL {
    pub fn new(node_id: &str) -> Self {
        let mut dir = PathBuf::from("storage");
        dir.push(node_id);

        let _ = create_dir_all(&dir);

        let mut file_path = dir;
        file_path.push("wal.log");

        Self { file_path }
    }

    // --------------------------------------------------
    // APPEND ENTRY (WRITE-AHEAD LOG RULE)
    // --------------------------------------------------
    pub fn append(&self, entry: &WalEntry) -> Result<(), String> {
        let serialized = serde_json::to_string(entry)
            .map_err(|e| format!("SERIALIZE_ERR: {e}"))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .map_err(|e| format!("OPEN_ERR: {e}"))?;

        writeln!(file, "{serialized}")
            .map_err(|e| format!("WRITE_ERR: {e}"))?;

        file.sync_all()
            .map_err(|e| format!("FSYNC_ERR: {e}"))?;

        Ok(())
    }

    // --------------------------------------------------
    // REPLAY ENTIRE LOG ON STARTUP
    // --------------------------------------------------
    pub fn replay(&self) -> Result<Vec<WalEntry>, String> {
        if !self.file_path.exists() {
            return Ok(vec![]);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.file_path)
            .map_err(|e| format!("OPEN_ERR: {e}"))?;

        let reader = BufReader::new(file);

        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|e| format!("READ_ERR: {e}"))?;

            if line.trim().is_empty() {
                continue;
            }

            let entry: WalEntry = serde_json::from_str(&line)
                .map_err(|e| format!("DESERIALIZE_ERR: {e}"))?;

            entries.push(entry);
        }

        Ok(entries)
    }
}

// ======================================================
// THREAD SAFE WRAPPER
// ======================================================

pub struct SharedWAL {
    pub inner: Arc<RwLock<DiskWAL>>,
}

impl SharedWAL {
    pub fn new(node_id: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(DiskWAL::new(node_id))),
        }
    }
}