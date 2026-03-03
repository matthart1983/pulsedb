//! Time-based partitioning for segment files.
//!
//! Data is partitioned into fixed-duration time windows (e.g., 1 hour).
//! Each partition maps to a directory containing segment files for that
//! time range, enabling efficient time-range pruning during queries.

use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};

/// Represents a single time partition.
#[derive(Debug, Clone)]
pub struct Partition {
    /// Partition identifier, e.g. "2024-01-15T14".
    pub id: String,
    /// Start of the time range (inclusive), nanosecond epoch.
    pub start_ns: i64,
    /// End of the time range (exclusive), nanosecond epoch.
    pub end_ns: i64,
    /// Segment file paths within this partition.
    pub segment_paths: Vec<PathBuf>,
}

/// Manages time-based partitioning of segment files.
pub struct PartitionManager {
    data_dir: PathBuf,
    duration_secs: u64,
}

impl PartitionManager {
    pub fn new(data_dir: &Path, duration_secs: u64) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            duration_secs,
        }
    }

    /// Compute the partition key for a given nanosecond timestamp.
    ///
    /// Returns a string like "2024-01-15T14" for hourly partitions.
    pub fn partition_key_for(&self, timestamp_ns: i64) -> String {
        let secs = timestamp_ns / 1_000_000_000;
        let aligned = secs - (secs % self.duration_secs as i64);
        let dt = DateTime::<Utc>::from_timestamp(aligned, 0)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        dt.format("%Y-%m-%dT%H").to_string()
    }

    /// Get the directory path for a given partition key.
    pub fn get_partition_dir(&self, key: &str) -> PathBuf {
        self.data_dir.join("partitions").join(key)
    }

    /// List all existing partitions by scanning the data directory.
    pub fn list_partitions(&self) -> Result<Vec<Partition>> {
        let partitions_dir = self.data_dir.join("partitions");
        if !partitions_dir.exists() {
            return Ok(Vec::new());
        }

        let mut partitions = Vec::new();
        let mut entries: Vec<_> = std::fs::read_dir(&partitions_dir)?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let id = entry.file_name().to_string_lossy().to_string();

            let mut segment_paths = Vec::new();
            for seg_entry in std::fs::read_dir(entry.path())? {
                let seg_entry = seg_entry?;
                let seg_path = seg_entry.path();
                if seg_path.extension().and_then(|e| e.to_str()) == Some("seg") {
                    segment_paths.push(seg_path);
                }
            }
            segment_paths.sort();

            partitions.push(Partition {
                id,
                start_ns: 0, // populated when reading segment headers
                end_ns: 0,
                segment_paths,
            });
        }

        Ok(partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn partition_key_hourly() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);

        // 2024-01-15 14:30:00 UTC in nanoseconds
        let ts_ns = 1705329000_000_000_000i64;
        let key = pm.partition_key_for(ts_ns);
        assert_eq!(key, "2024-01-15T14");
    }

    #[test]
    fn partition_key_same_hour() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);

        let ts1 = 1705329000_000_000_000i64; // 14:30
        let ts2 = 1705330799_000_000_000i64; // 14:59:59
        assert_eq!(pm.partition_key_for(ts1), pm.partition_key_for(ts2));
    }

    #[test]
    fn partition_key_different_hours() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);

        let ts1 = 1705329000_000_000_000i64; // 14:30
        let ts2 = 1705332600_000_000_000i64; // 15:30
        assert_ne!(pm.partition_key_for(ts1), pm.partition_key_for(ts2));
    }

    #[test]
    fn get_partition_dir_path() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);
        let pdir = pm.get_partition_dir("2024-01-15T14");
        assert!(pdir.ends_with("partitions/2024-01-15T14"));
    }

    #[test]
    fn list_empty_partitions() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);
        let parts = pm.list_partitions().unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn list_partitions_with_segments() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), 3600);

        // Create partition dirs with .seg files
        let p1 = pm.get_partition_dir("2024-01-15T14");
        std::fs::create_dir_all(&p1).unwrap();
        std::fs::write(p1.join("series1.seg"), b"fake").unwrap();
        std::fs::write(p1.join("series2.seg"), b"fake").unwrap();

        let p2 = pm.get_partition_dir("2024-01-15T15");
        std::fs::create_dir_all(&p2).unwrap();
        std::fs::write(p2.join("series1.seg"), b"fake").unwrap();

        let parts = pm.list_partitions().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].id, "2024-01-15T14");
        assert_eq!(parts[0].segment_paths.len(), 2);
        assert_eq!(parts[1].id, "2024-01-15T15");
        assert_eq!(parts[1].segment_paths.len(), 1);
    }
}
