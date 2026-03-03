//! Retention policy enforcement.
//!
//! Drops partitions whose time window is older than the configured
//! retention duration.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use tracing::info;

pub struct RetentionPolicy {
    data_dir: PathBuf,
    max_age_secs: u64,
}

impl RetentionPolicy {
    pub fn new(data_dir: &Path, max_age_secs: u64) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            max_age_secs,
        }
    }

    /// Drop partitions older than the retention period.
    /// Returns the number of partitions dropped.
    pub fn enforce(&self) -> Result<usize> {
        let partitions_dir = self.data_dir.join("partitions");
        if !partitions_dir.exists() {
            return Ok(0);
        }

        let cutoff = Utc::now().timestamp() - self.max_age_secs as i64;
        let mut dropped = 0;

        for entry in fs::read_dir(&partitions_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(partition_ts) = parse_partition_timestamp(&name) {
                if partition_ts < cutoff {
                    fs::remove_dir_all(entry.path()).with_context(|| {
                        format!("removing expired partition {}", entry.path().display())
                    })?;
                    info!(partition = name, "dropped expired partition");
                    dropped += 1;
                }
            }
        }

        Ok(dropped)
    }
}

/// Parse a partition key like "2024-01-15T14" into a Unix timestamp.
fn parse_partition_timestamp(key: &str) -> Option<i64> {
    // Partition keys are formatted as "%Y-%m-%dT%H"
    let dt = NaiveDateTime::parse_from_str(
        &format!("{}:00:00", key),
        "%Y-%m-%dT%H:%M:%S",
    )
    .ok()?;
    Some(dt.and_utc().timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn parse_partition_key() {
        let ts = parse_partition_timestamp("2024-01-15T14").unwrap();
        assert_eq!(ts, 1705327200); // 2024-01-15 14:00:00 UTC
    }

    #[test]
    fn invalid_partition_key_returns_none() {
        assert!(parse_partition_timestamp("not-a-date").is_none());
    }

    #[test]
    fn enforce_drops_old_partitions() {
        let dir = TempDir::new().unwrap();
        let partitions_dir = dir.path().join("partitions");

        // Create a very old partition
        let old = partitions_dir.join("2020-01-01T00");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("data.seg"), b"fake").unwrap();

        // Create a partition from today (should survive)
        let now = Utc::now().format("%Y-%m-%dT%H").to_string();
        let recent = partitions_dir.join(&now);
        fs::create_dir_all(&recent).unwrap();
        fs::write(recent.join("data.seg"), b"fake").unwrap();

        let policy = RetentionPolicy::new(dir.path(), 3600); // 1 hour retention
        let dropped = policy.enforce().unwrap();

        assert_eq!(dropped, 1);
        assert!(!old.exists());
        assert!(recent.exists());
    }

    #[test]
    fn enforce_no_partitions_dir() {
        let dir = TempDir::new().unwrap();
        let policy = RetentionPolicy::new(dir.path(), 3600);
        let dropped = policy.enforce().unwrap();
        assert_eq!(dropped, 0);
    }

    #[test]
    fn enforce_keeps_all_when_within_retention() {
        let dir = TempDir::new().unwrap();
        let partitions_dir = dir.path().join("partitions");

        let now = Utc::now().format("%Y-%m-%dT%H").to_string();
        let recent = partitions_dir.join(&now);
        fs::create_dir_all(&recent).unwrap();
        fs::write(recent.join("data.seg"), b"fake").unwrap();

        let policy = RetentionPolicy::new(dir.path(), 86400); // 24 hour retention
        let dropped = policy.enforce().unwrap();

        assert_eq!(dropped, 0);
        assert!(recent.exists());
    }
}
