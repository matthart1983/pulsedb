use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tracing::info;

use crate::index::{InvertedIndex, SeriesIndex};
use crate::model::{DataPoint, FieldValue, Tags};
use crate::query::aggregator::QueryResult;
use crate::storage::{PartitionManager, SegmentCache, SegmentMeta, SegmentWriter};

use super::config::EngineConfig;
use super::memtable::{FrozenMemTable, MemTable};
use super::wal::Wal;

/// Top-level write-path coordinator for PulseDB.
pub struct Database {
    config: EngineConfig,
    wal: RwLock<Wal>,
    active: RwLock<MemTable>,
    frozen: RwLock<Vec<FrozenMemTable>>,
    series_index: RwLock<SeriesIndex>,
    inverted_index: RwLock<InvertedIndex>,
    segment_cache: RwLock<SegmentCache>,
    partition_mgr: PartitionManager,
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

        let partition_mgr =
            PartitionManager::new(&config.data_dir, config.segment_duration_secs);

        Ok(Self {
            config,
            wal: RwLock::new(wal),
            active: RwLock::new(memtable),
            frozen: RwLock::new(Vec::new()),
            series_index: RwLock::new(SeriesIndex::new()),
            inverted_index: RwLock::new(InvertedIndex::new()),
            segment_cache: RwLock::new(SegmentCache::new()),
            partition_mgr,
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

    /// Execute a PulseQL query and return aggregated results.
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let stmt = crate::query::parser::Parser::new(sql)?.parse()?;
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let plan = {
            let inv = self.inverted_index.read();
            let cache = self.segment_cache.read();
            crate::query::planner::plan_query(&stmt, &inv, &cache, now_ns)?
        };

        let rows = {
            let cache = self.segment_cache.read();
            let active = self.active.read();
            crate::query::executor::execute(&plan, &cache, &active)?
        };

        crate::query::aggregator::aggregate(rows, &plan)
    }

    // --- internal ---

    /// Swap the active memtable for a fresh one and flush it to segment files.
    fn rotate_memtable(&self) -> Result<()> {
        let old = {
            let mut active = self.active.write();
            let new_table = MemTable::new();
            std::mem::replace(&mut *active, new_table)
        };

        let count = old.point_count();
        let frozen = old.freeze();

        self.flush_frozen(&frozen)?;

        {
            let mut list = self.frozen.write();
            list.push(frozen);
        }

        info!(points = count, "memtable frozen and flushed to segments");

        Ok(())
    }

    /// Flush a frozen memtable to compressed columnar segment files on disk.
    fn flush_frozen(&self, frozen: &FrozenMemTable) -> Result<()> {
        for (series_key, ts_fields) in frozen.iter_series() {
            let (timestamps, fields) = columnar_from_series(ts_fields);
            if timestamps.is_empty() {
                continue;
            }

            // Index the series.
            let series_id = self.series_index.write().get_or_create(series_key);

            let tags = parse_tags_from_series_key(series_key);
            self.inverted_index.write().index_series(series_id, &tags);

            // Determine partition from the first timestamp.
            let partition_key = self.partition_mgr.partition_key_for(timestamps[0]);
            let partition_dir = self.partition_mgr.get_partition_dir(&partition_key);

            // Build a filesystem-safe segment filename.
            let safe_name = series_key.replace(',', "_").replace('=', "-");
            let segment_path = partition_dir.join(format!("{safe_name}.seg"));

            SegmentWriter::write_segment(&segment_path, series_key, &timestamps, &fields)
                .with_context(|| {
                    format!("writing segment for series '{series_key}'")
                })?;

            // Register in the segment cache.
            let min_time = timestamps[0];
            let max_time = timestamps[timestamps.len() - 1];
            self.segment_cache.write().add(SegmentMeta {
                path: segment_path,
                series_key: series_key.clone(),
                min_time,
                max_time,
                point_count: timestamps.len() as u64,
            });
        }

        // Truncate the WAL after all series are successfully flushed.
        self.wal.write().truncate().context("truncating WAL after flush")?;

        Ok(())
    }
}

/// Parse tags from a series key string like `measurement,tag1=val1,tag2=val2`.
/// The first comma-separated part is the measurement and is skipped.
fn parse_tags_from_series_key(series_key: &str) -> Tags {
    let mut tags = BTreeMap::new();
    for part in series_key.split(',').skip(1) {
        if let Some((k, v)) = part.split_once('=') {
            tags.insert(k.to_string(), v.to_string());
        }
    }
    tags
}

/// Convert the per-series data from the memtable's row-oriented format into the
/// columnar format expected by [`SegmentWriter`].
///
/// Input:  `BTreeMap<timestamp, BTreeMap<field_name, FieldValue>>`
/// Output: `(Vec<timestamp>, BTreeMap<field_name, Vec<FieldValue>>)`
fn columnar_from_series(
    ts_fields: &BTreeMap<i64, BTreeMap<String, FieldValue>>,
) -> (Vec<i64>, BTreeMap<String, Vec<FieldValue>>) {
    // Collect all field names across all timestamps.
    let mut field_names: Vec<String> = Vec::new();
    for fields in ts_fields.values() {
        for name in fields.keys() {
            if !field_names.contains(name) {
                field_names.push(name.clone());
            }
        }
    }
    field_names.sort();

    let mut timestamps = Vec::with_capacity(ts_fields.len());
    let mut columns: BTreeMap<String, Vec<FieldValue>> = BTreeMap::new();
    for name in &field_names {
        columns.insert(name.clone(), Vec::with_capacity(ts_fields.len()));
    }

    for (&ts, fields) in ts_fields {
        timestamps.push(ts);
        for name in &field_names {
            let value = fields
                .get(name)
                .cloned()
                .unwrap_or(FieldValue::Float(0.0));
            columns.get_mut(name).unwrap().push(value);
        }
    }

    (timestamps, columns)
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

    #[test]
    fn flush_writes_segment_files_to_disk() {
        use crate::storage::SegmentReader;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path());
        cfg.memtable_size_bytes = 1; // trigger flush immediately

        let db = Database::open(cfg).unwrap();

        // All points share the same series key so they land in one segment.
        let points: Vec<DataPoint> = (0..5)
            .map(|i| DataPoint {
                measurement: "cpu".into(),
                tags: BTreeMap::from([("host".into(), "web1".into())]),
                fields: BTreeMap::from([("usage".into(), FieldValue::Float(i as f64 * 10.0))]),
                timestamp: 1_000_000_000 + i as i64,
            })
            .collect();

        db.write(points).unwrap();

        // Segment cache should have an entry.
        let cache = db.segment_cache.read();
        assert_eq!(cache.len(), 1);

        let meta = &cache.segments_for_series("cpu,host=web1")[0];
        assert!(meta.path.exists(), "segment file should exist on disk");
        assert_eq!(meta.point_count, 5);

        // Read the segment back and verify contents.
        let reader = SegmentReader::open(&meta.path).unwrap();
        assert_eq!(reader.point_count(), 5);
        assert_eq!(reader.series_key(), "cpu,host=web1");

        let ts = reader.read_timestamps().unwrap();
        assert_eq!(ts, vec![1_000_000_000, 1_000_000_001, 1_000_000_002, 1_000_000_003, 1_000_000_004]);

        let vals = reader.read_column("usage").unwrap();
        assert_eq!(vals.len(), 5);
        assert_eq!(vals[0], FieldValue::Float(0.0));
        assert_eq!(vals[4], FieldValue::Float(40.0));
    }

    #[test]
    fn wal_truncated_after_flush() {
        use super::super::wal::Wal;
        use super::super::config::FsyncPolicy;

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path());
        cfg.memtable_size_bytes = 1; // trigger flush immediately

        let db = Database::open(cfg.clone()).unwrap();
        db.write(make_points(3)).unwrap();

        // The flush should have truncated the WAL.
        // Open a fresh WAL and recover — should find nothing.
        let wal_dir = cfg.wal_dir();
        let mut wal = Wal::open(&wal_dir, FsyncPolicy::None).unwrap();
        let recovered = wal.recover().unwrap();
        assert!(recovered.is_empty(), "WAL should be empty after flush");
    }

    #[test]
    fn columnar_from_series_helper() {
        let mut ts_fields: BTreeMap<i64, BTreeMap<String, FieldValue>> = BTreeMap::new();
        ts_fields.insert(
            100,
            BTreeMap::from([
                ("a".into(), FieldValue::Float(1.0)),
                ("b".into(), FieldValue::Integer(2)),
            ]),
        );
        ts_fields.insert(
            200,
            BTreeMap::from([
                ("a".into(), FieldValue::Float(3.0)),
                ("b".into(), FieldValue::Integer(4)),
            ]),
        );

        let (timestamps, fields) = columnar_from_series(&ts_fields);
        assert_eq!(timestamps, vec![100, 200]);
        assert_eq!(
            fields["a"],
            vec![FieldValue::Float(1.0), FieldValue::Float(3.0)]
        );
        assert_eq!(
            fields["b"],
            vec![FieldValue::Integer(2), FieldValue::Integer(4)]
        );
    }

    #[test]
    fn query_with_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path());
        cfg.memtable_size_bytes = 1; // trigger flush immediately

        let db = Database::open(cfg).unwrap();

        // Write data points that will be flushed to segments.
        // Timestamps are nanoseconds; use 5-second intervals for GROUP BY time(5s).
        let base_ns = 1_000_000_000_000i64; // 1e12 ns
        let sec = 1_000_000_000i64;
        let points: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint {
                measurement: "cpu".into(),
                tags: BTreeMap::from([("host".into(), "web1".into())]),
                fields: BTreeMap::from([("usage".into(), FieldValue::Float(i as f64 * 10.0))]),
                timestamp: base_ns + i as i64 * sec, // one point per second
            })
            .collect();

        db.write(points).unwrap();

        // Query: SELECT mean(usage) FROM cpu WHERE host = 'web1' GROUP BY time(5s)
        let result = db
            .query("SELECT mean(usage) FROM cpu WHERE host = 'web1' GROUP BY time(5s)")
            .unwrap();

        assert_eq!(result.name, "cpu");
        // 10 points spread over 10 seconds → 2 buckets of 5s each
        assert_eq!(result.rows.len(), 2);

        // First bucket: values 0, 10, 20, 30, 40 → mean = 20
        assert_eq!(result.rows[0].values["mean(usage)"], 20.0);
        // Second bucket: values 50, 60, 70, 80, 90 → mean = 70
        assert_eq!(result.rows[1].values["mean(usage)"], 70.0);
    }

    #[test]
    fn parse_tags_from_series_key_helper() {
        let tags = parse_tags_from_series_key("cpu,host=web1,region=us");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags["host"], "web1");
        assert_eq!(tags["region"], "us");

        let empty = parse_tags_from_series_key("cpu");
        assert!(empty.is_empty());
    }
}
