//! Background segment compaction.
//!
//! Merges small segments within the same partition into larger ones,
//! re-compresses data, and drops tombstoned entries.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::info;

use crate::model::FieldValue;
use crate::storage::segment::{SegmentReader, SegmentWriter};

pub struct Compactor {
    data_dir: PathBuf,
}

#[derive(Debug, Default)]
pub struct CompactionStats {
    pub segments_read: usize,
    pub segments_written: usize,
    pub segments_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

impl CompactionStats {
    fn merge(&mut self, other: &CompactionStats) {
        self.segments_read += other.segments_read;
        self.segments_written += other.segments_written;
        self.segments_removed += other.segments_removed;
        self.bytes_before += other.bytes_before;
        self.bytes_after += other.bytes_after;
    }
}

impl Compactor {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Compact all partitions — merge multiple segments for the same series
    /// within a partition into a single segment.
    pub fn compact_all(&self) -> Result<CompactionStats> {
        let partitions_dir = self.data_dir.join("partitions");
        if !partitions_dir.exists() {
            return Ok(CompactionStats::default());
        }

        let mut total = CompactionStats::default();

        let mut entries: Vec<_> = fs::read_dir(&partitions_dir)?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            if entry.file_type()?.is_dir() {
                let stats = self.compact_partition(&entry.path())?;
                total.merge(&stats);
            }
        }

        Ok(total)
    }

    /// Compact a single partition directory.
    pub fn compact_partition(&self, partition_dir: &Path) -> Result<CompactionStats> {
        let mut stats = CompactionStats::default();

        // 1. List all .seg files
        let seg_files = list_segment_files(partition_dir)?;
        if seg_files.is_empty() {
            return Ok(stats);
        }

        // 2. Group segments by series_key
        let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for path in &seg_files {
            let reader = SegmentReader::open(path)
                .with_context(|| format!("opening segment {}", path.display()))?;
            groups
                .entry(reader.series_key().to_string())
                .or_default()
                .push(path.clone());
        }

        // 3. For each series with multiple segments, merge them
        for (series_key, paths) in &groups {
            if paths.len() < 2 {
                continue;
            }

            let bytes_before: u64 = paths
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            stats.bytes_before += bytes_before;
            stats.segments_read += paths.len();

            // a. Read all timestamps and fields from each segment
            let mut all_entries: Vec<(i64, BTreeMap<String, FieldValue>)> = Vec::new();

            for path in paths {
                let reader = SegmentReader::open(path)?;
                let timestamps = reader.read_timestamps()?;
                let field_names: Vec<String> =
                    reader.field_names().iter().map(|s| s.to_string()).collect();
                let mut field_columns: Vec<Vec<FieldValue>> = Vec::new();
                for name in &field_names {
                    field_columns.push(reader.read_column(name)?);
                }

                for (i, &ts) in timestamps.iter().enumerate() {
                    let mut fields = BTreeMap::new();
                    for (j, name) in field_names.iter().enumerate() {
                        fields.insert(name.clone(), field_columns[j][i].clone());
                    }
                    all_entries.push((ts, fields));
                }
            }

            // b. Sort by timestamp and deduplicate (last value wins for overlapping timestamps)
            all_entries.sort_by_key(|(ts, _)| *ts);

            let mut merged_timestamps: Vec<i64> = Vec::new();
            let mut merged_rows: Vec<BTreeMap<String, FieldValue>> = Vec::new();

            for (ts, fields) in all_entries {
                if let Some(last_ts) = merged_timestamps.last() {
                    if *last_ts == ts {
                        // Overlapping timestamp — replace with latest value
                        let last = merged_rows.last_mut().unwrap();
                        for (k, v) in fields {
                            last.insert(k, v);
                        }
                        continue;
                    }
                }
                merged_timestamps.push(ts);
                merged_rows.push(fields);
            }

            // Build columnar field arrays
            let all_field_names: Vec<String> = {
                let mut names: Vec<String> = merged_rows
                    .iter()
                    .flat_map(|r| r.keys().cloned())
                    .collect();
                names.sort();
                names.dedup();
                names
            };

            let mut merged_fields: BTreeMap<String, Vec<FieldValue>> = BTreeMap::new();
            for name in &all_field_names {
                let col: Vec<FieldValue> = merged_rows
                    .iter()
                    .map(|r| {
                        r.get(name)
                            .cloned()
                            .unwrap_or(FieldValue::Float(0.0))
                    })
                    .collect();
                merged_fields.insert(name.clone(), col);
            }

            // c. Write merged segment to a temp file
            let temp_path = partition_dir.join(format!("{}.seg.tmp", sanitize_key(series_key)));
            SegmentWriter::write_segment(
                &temp_path,
                series_key,
                &merged_timestamps,
                &merged_fields,
            )?;

            // d. Delete old segment files
            for path in paths {
                fs::remove_file(path)
                    .with_context(|| format!("removing old segment {}", path.display()))?;
            }
            stats.segments_removed += paths.len();

            // e. Rename temp file to final name
            let final_path = partition_dir.join(format!("{}.seg", sanitize_key(series_key)));
            fs::rename(&temp_path, &final_path)
                .with_context(|| format!("renaming temp segment to {}", final_path.display()))?;

            let bytes_after = fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
            stats.bytes_after += bytes_after;
            stats.segments_written += 1;

            info!(
                series = series_key,
                merged = paths.len(),
                "compacted series"
            );
        }

        Ok(stats)
    }
}

fn list_segment_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("seg") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn sanitize_key(key: &str) -> String {
    key.replace(|c: char| !c.is_alphanumeric() && c != '_' && c != '-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_float_fields(
        name: &str,
        values: &[f64],
    ) -> BTreeMap<String, Vec<FieldValue>> {
        let mut fields = BTreeMap::new();
        fields.insert(
            name.to_string(),
            values.iter().map(|&v| FieldValue::Float(v)).collect(),
        );
        fields
    }

    fn write_test_segment(
        dir: &Path,
        filename: &str,
        series_key: &str,
        timestamps: &[i64],
        field_name: &str,
        values: &[f64],
    ) {
        let path = dir.join(filename);
        let fields = make_float_fields(field_name, values);
        SegmentWriter::write_segment(&path, series_key, timestamps, &fields).unwrap();
    }

    #[test]
    fn compact_two_segments_same_series() {
        let dir = TempDir::new().unwrap();
        let partition = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&partition).unwrap();

        write_test_segment(
            &partition,
            "seg1.seg",
            "cpu,host=a",
            &[100, 200, 300],
            "value",
            &[1.0, 2.0, 3.0],
        );
        write_test_segment(
            &partition,
            "seg2.seg",
            "cpu,host=a",
            &[400, 500, 600],
            "value",
            &[4.0, 5.0, 6.0],
        );

        let compactor = Compactor::new(dir.path());
        let stats = compactor.compact_partition(&partition).unwrap();

        assert_eq!(stats.segments_read, 2);
        assert_eq!(stats.segments_written, 1);
        assert_eq!(stats.segments_removed, 2);

        let seg_files = list_segment_files(&partition).unwrap();
        assert_eq!(seg_files.len(), 1);
    }

    #[test]
    fn compact_preserves_data() {
        let dir = TempDir::new().unwrap();
        let partition = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&partition).unwrap();

        write_test_segment(
            &partition,
            "seg1.seg",
            "cpu,host=a",
            &[100, 200, 300],
            "value",
            &[1.0, 2.0, 3.0],
        );
        write_test_segment(
            &partition,
            "seg2.seg",
            "cpu,host=a",
            &[400, 500, 600],
            "value",
            &[4.0, 5.0, 6.0],
        );

        let compactor = Compactor::new(dir.path());
        compactor.compact_partition(&partition).unwrap();

        let seg_files = list_segment_files(&partition).unwrap();
        let reader = SegmentReader::open(&seg_files[0]).unwrap();
        assert_eq!(reader.series_key(), "cpu,host=a");
        assert_eq!(reader.point_count(), 6);

        let ts = reader.read_timestamps().unwrap();
        assert_eq!(ts, vec![100, 200, 300, 400, 500, 600]);

        let vals = reader.read_column("value").unwrap();
        let floats: Vec<f64> = vals
            .iter()
            .map(|v| match v {
                FieldValue::Float(f) => *f,
                _ => panic!("expected float"),
            })
            .collect();
        assert_eq!(floats, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    #[test]
    fn compact_non_overlapping_ranges() {
        let dir = TempDir::new().unwrap();
        let partition = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&partition).unwrap();

        write_test_segment(
            &partition,
            "seg1.seg",
            "mem,host=b",
            &[1000, 2000],
            "used",
            &[50.0, 60.0],
        );
        write_test_segment(
            &partition,
            "seg2.seg",
            "mem,host=b",
            &[5000, 6000],
            "used",
            &[70.0, 80.0],
        );

        let compactor = Compactor::new(dir.path());
        compactor.compact_partition(&partition).unwrap();

        let seg_files = list_segment_files(&partition).unwrap();
        let reader = SegmentReader::open(&seg_files[0]).unwrap();
        let ts = reader.read_timestamps().unwrap();
        assert_eq!(ts, vec![1000, 2000, 5000, 6000]);
    }

    #[test]
    fn compact_overlapping_timestamps_dedup() {
        let dir = TempDir::new().unwrap();
        let partition = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&partition).unwrap();

        // seg1 has ts 100,200,300 with values 1.0,2.0,3.0
        write_test_segment(
            &partition,
            "seg1.seg",
            "cpu,host=a",
            &[100, 200, 300],
            "value",
            &[1.0, 2.0, 3.0],
        );
        // seg2 has ts 200,300,400 with values 20.0,30.0,40.0
        // ts 200 and 300 overlap — seg2 values (later in sort) should win
        write_test_segment(
            &partition,
            "seg2.seg",
            "cpu,host=a",
            &[200, 300, 400],
            "value",
            &[20.0, 30.0, 40.0],
        );

        let compactor = Compactor::new(dir.path());
        compactor.compact_partition(&partition).unwrap();

        let seg_files = list_segment_files(&partition).unwrap();
        let reader = SegmentReader::open(&seg_files[0]).unwrap();
        assert_eq!(reader.point_count(), 4);

        let ts = reader.read_timestamps().unwrap();
        assert_eq!(ts, vec![100, 200, 300, 400]);

        let vals = reader.read_column("value").unwrap();
        let floats: Vec<f64> = vals
            .iter()
            .map(|v| match v {
                FieldValue::Float(f) => *f,
                _ => panic!("expected float"),
            })
            .collect();
        assert_eq!(floats, vec![1.0, 20.0, 30.0, 40.0]);
    }

    #[test]
    fn noop_single_segment_per_series() {
        let dir = TempDir::new().unwrap();
        let partition = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&partition).unwrap();

        write_test_segment(
            &partition,
            "seg1.seg",
            "cpu,host=a",
            &[100, 200, 300],
            "value",
            &[1.0, 2.0, 3.0],
        );
        write_test_segment(
            &partition,
            "seg2.seg",
            "cpu,host=b",
            &[100, 200, 300],
            "value",
            &[10.0, 20.0, 30.0],
        );

        let compactor = Compactor::new(dir.path());
        let stats = compactor.compact_partition(&partition).unwrap();

        assert_eq!(stats.segments_read, 0);
        assert_eq!(stats.segments_written, 0);
        assert_eq!(stats.segments_removed, 0);

        // Both original files should still exist
        let seg_files = list_segment_files(&partition).unwrap();
        assert_eq!(seg_files.len(), 2);
    }

    #[test]
    fn compact_all_across_partitions() {
        let dir = TempDir::new().unwrap();

        let p1 = dir.path().join("partitions").join("2024-01-15T14");
        fs::create_dir_all(&p1).unwrap();
        write_test_segment(&p1, "a1.seg", "cpu,host=a", &[100, 200], "value", &[1.0, 2.0]);
        write_test_segment(&p1, "a2.seg", "cpu,host=a", &[300, 400], "value", &[3.0, 4.0]);

        let p2 = dir.path().join("partitions").join("2024-01-15T15");
        fs::create_dir_all(&p2).unwrap();
        write_test_segment(&p2, "b1.seg", "cpu,host=b", &[500, 600], "value", &[5.0, 6.0]);
        write_test_segment(&p2, "b2.seg", "cpu,host=b", &[700, 800], "value", &[7.0, 8.0]);

        let compactor = Compactor::new(dir.path());
        let stats = compactor.compact_all().unwrap();

        assert_eq!(stats.segments_read, 4);
        assert_eq!(stats.segments_written, 2);
        assert_eq!(stats.segments_removed, 4);
    }
}
