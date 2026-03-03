use std::path::PathBuf;

use serde::Deserialize;

/// Controls when WAL entries are fsynced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FsyncPolicy {
    /// Fsync after every write.
    Every,
    /// Fsync periodically in batches (default).
    Batch,
    /// Never explicitly fsync — rely on OS page cache.
    None,
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        Self::Batch
    }
}

/// Top-level configuration for the PulseDB engine.
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Root directory for data files.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Directory for WAL files. Defaults to `<data_dir>/wal`.
    #[serde(default)]
    pub wal_dir: Option<PathBuf>,

    /// Maximum size (in bytes) of the active memtable before it is frozen.
    /// Default: 64 MiB.
    #[serde(default = "default_memtable_size_bytes")]
    pub memtable_size_bytes: usize,

    /// Fsync policy for WAL writes.
    #[serde(default)]
    pub wal_fsync: FsyncPolicy,

    /// Duration (in seconds) of each time-based segment partition.
    /// Default: 3600 (1 hour).
    #[serde(default = "default_segment_duration_secs")]
    pub segment_duration_secs: u64,

    /// Retention duration in seconds. Partitions older than this are deleted.
    /// 0 means no retention (keep everything).
    #[serde(default)]
    pub retention_secs: u64,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./pulsedb_data")
}

fn default_memtable_size_bytes() -> usize {
    67_108_864 // 64 MiB
}

fn default_segment_duration_secs() -> u64 {
    3600
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            wal_dir: None,
            memtable_size_bytes: default_memtable_size_bytes(),
            wal_fsync: FsyncPolicy::default(),
            segment_duration_secs: default_segment_duration_secs(),
            retention_secs: 0,
        }
    }
}

impl EngineConfig {
    /// Returns the effective WAL directory, falling back to `<data_dir>/wal`.
    pub fn wal_dir(&self) -> PathBuf {
        self.wal_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("wal"))
    }
}
