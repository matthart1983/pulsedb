use std::fs;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tracing::info;

use crate::model::DataPoint;

use super::config::EngineConfig;
use super::memtable::{FrozenMemTable, MemTable};
use super::wal::Wal;

/// Top-level write-path coordinator for PulseDB.
pub struct Database {
    config: EngineConfig,
    wal: RwLock<Wal>,
    active: RwLock<MemTable>,
    frozen: RwLock<Vec<FrozenMemTable>>,
}

impl Database {
    /// Open (or create) a database at the configured directories.
    ///
    /// Replays the WAL to recover any data that was not flushed.
    pub fn open(config: EngineConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir).context("creating data directory")?;

        let wal_dir = config.wal_dir();
        let mut wal = Wal::open(&wal_dir, config.wal_fsync).context("opening WAL")?;

        // Recover unflushed data from the WAL.
        let recovered = wal.recover().context("recovering WAL")?;

        let mut memtable = MemTable::new();
        if !recovered.is_empty() {
            info!(points = recovered.len(), "recovered data points from WAL");
            for point in recovered {
                memtable.insert(point);
            }
        }

        Ok(Self {
            config,
            wal: RwLock::new(wal),
            active: RwLock::new(memtable),
            frozen: RwLock::new(Vec::new()),
        })
    }

    /// Write a batch of data points.
    ///
    /// The points are first durably appended to the WAL, then inserted into
    /// the active memtable. If the memtable exceeds the size threshold it is
    /// frozen (actual flush to segments is not yet implemented).
    pub fn write(&self, points: Vec<DataPoint>) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        // WAL first for durability.
        {
            let mut wal = self.wal.write();
            wal.append(&points).context("WAL append")?;
        }

        // Insert into the active memtable.
        {
            let mut active = self.active.write();
            for point in points {
                active.insert(point);
            }
        }

        if self.should_flush() {
            self.rotate_memtable()?;
        }

        Ok(())
    }

    /// Returns `true` when the active memtable has grown past the configured
    /// threshold and should be frozen.
    pub fn should_flush(&self) -> bool {
        let active = self.active.read();
        active.size_bytes() >= self.config.memtable_size_bytes
    }

    /// Total number of data points in the active memtable.
    pub fn point_count(&self) -> usize {
        self.active.read().point_count()
    }

    // --- internal ---

    /// Swap the active memtable for a fresh one and push the old one onto the
    /// frozen list. In a full implementation this would trigger a background
    /// flush to segment files.
    fn rotate_memtable(&self) -> Result<()> {
        let old = {
            let mut active = self.active.write();
            let new_table = MemTable::new();
            std::mem::replace(&mut *active, new_table)
        };

        let count = old.point_count();
        let frozen = old.freeze();

        {
            let mut list = self.frozen.write();
            list.push(frozen);
        }

        info!(
            points = count,
            pending = self.frozen.read().len(),
            "memtable frozen (segment flush not yet implemented)"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::model::FieldValue;

    fn test_config(dir: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: dir.to_path_buf(),
            memtable_size_bytes: 4096, // small threshold for tests
            ..Default::default()
        }
    }

    fn make_points(n: usize) -> Vec<DataPoint> {
        (0..n)
            .map(|i| DataPoint {
                measurement: "cpu".into(),
                tags: BTreeMap::from([("host".into(), format!("srv-{i}"))]),
                fields: BTreeMap::from([("usage".into(), FieldValue::Float(i as f64))]),
                timestamp: 1_700_000_000 + i as i64,
            })
            .collect()
    }

    #[test]
    fn open_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(test_config(dir.path())).unwrap();

        db.write(make_points(10)).unwrap();
        assert_eq!(db.point_count(), 10);
    }

    #[test]
    fn wal_recovery_on_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let db = Database::open(test_config(dir.path())).unwrap();
            db.write(make_points(5)).unwrap();
            // Drop without explicit flush — data lives in WAL.
        }

        let db2 = Database::open(test_config(dir.path())).unwrap();
        assert_eq!(db2.point_count(), 5);
    }

    #[test]
    fn flush_threshold_triggers_freeze() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path());
        cfg.memtable_size_bytes = 1; // trigger immediately

        let db = Database::open(cfg).unwrap();
        db.write(make_points(3)).unwrap();

        // After rotation the active memtable should be empty.
        assert_eq!(db.point_count(), 0);

        // Frozen list should have one entry.
        let frozen = db.frozen.read();
        assert_eq!(frozen.len(), 1);
        assert_eq!(frozen[0].point_count(), 3);
    }

    #[test]
    fn write_empty_batch_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(test_config(dir.path())).unwrap();
        db.write(vec![]).unwrap();
        assert_eq!(db.point_count(), 0);
    }
}
