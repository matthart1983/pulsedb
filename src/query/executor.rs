//! Query executor — scans segments and the memtable to produce raw rows.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::engine::memtable::MemTable;
use crate::model::FieldValue;
use crate::query::ast::FieldExpr;
use crate::query::planner::QueryPlan;
use crate::storage::{SegmentCache, SegmentReader};

/// Raw row of data from a scan.
#[derive(Debug, Clone)]
pub struct ScanRow {
    pub timestamp: i64,
    pub series_key: String,
    pub fields: BTreeMap<String, FieldValue>,
}

/// Execute a query plan against segments and the memtable.
pub fn execute(
    plan: &QueryPlan,
    segment_cache: &SegmentCache,
    active_memtable: &MemTable,
) -> Result<Vec<ScanRow>> {
    let (min_time, max_time) = plan.time_range;
    let mut rows = Vec::new();

    for series_key in &plan.series_keys {
        // Scan matching segments.
        let segments = segment_cache.segments_for_range(series_key, min_time, max_time);
        for meta in segments {
            let reader = SegmentReader::open(&meta.path)?;
            let timestamps = reader.read_timestamps()?;

            // Determine which fields to read.
            let field_names = requested_field_names(plan, &reader);

            // Read all requested columns.
            let mut columns: Vec<(String, Vec<FieldValue>)> = Vec::new();
            for name in &field_names {
                if let Ok(col) = reader.read_column(name) {
                    columns.push((name.clone(), col));
                }
            }

            // Produce ScanRows for matching timestamps.
            for (i, &ts) in timestamps.iter().enumerate() {
                if ts < min_time || ts > max_time {
                    continue;
                }
                let mut fields = BTreeMap::new();
                for (name, col) in &columns {
                    if i < col.len() {
                        fields.insert(name.clone(), col[i].clone());
                    }
                }
                rows.push(ScanRow {
                    timestamp: ts,
                    series_key: series_key.clone(),
                    fields,
                });
            }
        }

        // Scan the active memtable.
        for (key, ts_fields) in active_memtable.iter_series() {
            if key != series_key {
                continue;
            }
            for (&ts, fields) in ts_fields {
                if ts < min_time || ts > max_time {
                    continue;
                }
                let selected = select_fields(fields, plan);
                rows.push(ScanRow {
                    timestamp: ts,
                    series_key: series_key.clone(),
                    fields: selected,
                });
            }
        }
    }

    // Sort by timestamp.
    if plan.order_desc {
        rows.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    } else {
        rows.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }

    // Apply offset and limit.
    let offset = plan.offset.unwrap_or(0) as usize;
    if offset > 0 {
        if offset >= rows.len() {
            rows.clear();
        } else {
            rows = rows.split_off(offset);
        }
    }
    if let Some(limit) = plan.limit {
        rows.truncate(limit as usize);
    }

    Ok(rows)
}

/// Determine the field names to read from a segment based on the query plan.
fn requested_field_names(plan: &QueryPlan, reader: &SegmentReader) -> Vec<String> {
    let mut names = Vec::new();
    for field_expr in &plan.fields {
        match field_expr {
            FieldExpr::Wildcard => {
                for name in reader.field_names() {
                    if !names.contains(&name.to_string()) {
                        names.push(name.to_string());
                    }
                }
            }
            FieldExpr::Field(name) => {
                if !names.contains(name) {
                    names.push(name.clone());
                }
            }
            FieldExpr::Aggregate { field, .. } => {
                if !names.contains(field) {
                    names.push(field.clone());
                }
            }
        }
    }
    names
}

/// Select fields from a memtable row based on the query plan.
fn select_fields(
    fields: &BTreeMap<String, FieldValue>,
    plan: &QueryPlan,
) -> BTreeMap<String, FieldValue> {
    let is_wildcard = plan.fields.iter().any(|f| matches!(f, FieldExpr::Wildcard));
    if is_wildcard {
        return fields.clone();
    }

    let mut selected = BTreeMap::new();
    for field_expr in &plan.fields {
        let name = match field_expr {
            FieldExpr::Field(n) => n,
            FieldExpr::Aggregate { field, .. } => field,
            FieldExpr::Wildcard => continue,
        };
        if let Some(val) = fields.get(name) {
            selected.insert(name.clone(), val.clone());
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FieldValue;
    use crate::query::ast::*;
    use crate::query::planner::QueryPlan;
    use crate::storage::{SegmentCache, SegmentMeta, SegmentWriter};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn write_test_segment(
        dir: &std::path::Path,
        series_key: &str,
        timestamps: &[i64],
        field_name: &str,
        values: &[f64],
    ) -> std::path::PathBuf {
        let path = dir.join(format!(
            "{}_{}.seg",
            series_key.replace(',', "_").replace('=', "-"),
            timestamps[0]
        ));
        let mut fields = BTreeMap::new();
        fields.insert(
            field_name.to_string(),
            values.iter().map(|&v| FieldValue::Float(v)).collect(),
        );
        SegmentWriter::write_segment(&path, series_key, timestamps, &fields).unwrap();
        path
    }

    fn make_plan(series_keys: Vec<String>) -> QueryPlan {
        QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Wildcard],
            series_keys,
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        }
    }

    #[test]
    fn execute_from_segments() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = (100..105).collect();
        let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let path = write_test_segment(dir.path(), "cpu,host=a", &timestamps, "usage", &values);

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "cpu,host=a".into(),
            min_time: 100,
            max_time: 104,
            point_count: 5,
        });

        let memtable = MemTable::new();
        let plan = make_plan(vec!["cpu,host=a".into()]);

        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].timestamp, 100);
        assert_eq!(rows[4].timestamp, 104);
        assert_eq!(rows[0].fields["usage"], FieldValue::Float(1.0));
    }

    #[test]
    fn execute_from_memtable() {
        let cache = SegmentCache::new();
        let mut memtable = MemTable::new();
        for i in 0..3 {
            memtable.insert(crate::model::DataPoint {
                measurement: "cpu".into(),
                tags: [("host".into(), "a".into())].into_iter().collect(),
                fields: [("usage".into(), FieldValue::Float(i as f64 * 10.0))]
                    .into_iter()
                    .collect(),
                timestamp: 200 + i,
            });
        }

        let plan = make_plan(vec!["cpu,host=a".into()]);
        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].timestamp, 200);
    }

    #[test]
    fn execute_with_time_filter() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = (100..110).collect();
        let values: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let path = write_test_segment(dir.path(), "cpu,host=a", &timestamps, "usage", &values);

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "cpu,host=a".into(),
            min_time: 100,
            max_time: 109,
            point_count: 10,
        });

        let memtable = MemTable::new();
        let mut plan = make_plan(vec!["cpu,host=a".into()]);
        plan.time_range = (103, 106);

        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].timestamp, 103);
        assert_eq!(rows[3].timestamp, 106);
    }

    #[test]
    fn execute_with_limit_offset() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = (0..20).collect();
        let values: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let path = write_test_segment(dir.path(), "cpu,host=a", &timestamps, "usage", &values);

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "cpu,host=a".into(),
            min_time: 0,
            max_time: 19,
            point_count: 20,
        });

        let memtable = MemTable::new();
        let mut plan = make_plan(vec!["cpu,host=a".into()]);
        plan.limit = Some(5);
        plan.offset = Some(3);

        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].timestamp, 3);
        assert_eq!(rows[4].timestamp, 7);
    }

    #[test]
    fn execute_order_desc() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = (100..105).collect();
        let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let path = write_test_segment(dir.path(), "cpu,host=a", &timestamps, "usage", &values);

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "cpu,host=a".into(),
            min_time: 100,
            max_time: 104,
            point_count: 5,
        });

        let memtable = MemTable::new();
        let mut plan = make_plan(vec!["cpu,host=a".into()]);
        plan.order_desc = true;

        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows[0].timestamp, 104);
        assert_eq!(rows[4].timestamp, 100);
    }

    #[test]
    fn execute_merges_segment_and_memtable() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = vec![100, 102, 104];
        let values: Vec<f64> = vec![1.0, 3.0, 5.0];
        let path = write_test_segment(dir.path(), "cpu,host=a", &timestamps, "usage", &values);

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "cpu,host=a".into(),
            min_time: 100,
            max_time: 104,
            point_count: 3,
        });

        let mut memtable = MemTable::new();
        for ts in [101, 103, 105] {
            memtable.insert(crate::model::DataPoint {
                measurement: "cpu".into(),
                tags: [("host".into(), "a".into())].into_iter().collect(),
                fields: [("usage".into(), FieldValue::Float(ts as f64))]
                    .into_iter()
                    .collect(),
                timestamp: ts,
            });
        }

        let plan = make_plan(vec!["cpu,host=a".into()]);
        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 6);
        let ts: Vec<i64> = rows.iter().map(|r| r.timestamp).collect();
        assert_eq!(ts, vec![100, 101, 102, 103, 104, 105]);
    }

    #[test]
    fn execute_specific_fields() {
        let dir = TempDir::new().unwrap();
        let timestamps: Vec<i64> = vec![100, 101, 102];
        let mut fields = BTreeMap::new();
        fields.insert(
            "cpu".to_string(),
            vec![
                FieldValue::Float(1.0),
                FieldValue::Float(2.0),
                FieldValue::Float(3.0),
            ],
        );
        fields.insert(
            "mem".to_string(),
            vec![
                FieldValue::Float(10.0),
                FieldValue::Float(20.0),
                FieldValue::Float(30.0),
            ],
        );
        let path = dir.path().join("multi.seg");
        SegmentWriter::write_segment(&path, "sys,host=a", &timestamps, &fields).unwrap();

        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path,
            series_key: "sys,host=a".into(),
            min_time: 100,
            max_time: 102,
            point_count: 3,
        });

        let memtable = MemTable::new();
        let plan = QueryPlan {
            measurement: "sys".into(),
            fields: vec![FieldExpr::Field("cpu".into())],
            series_keys: vec!["sys,host=a".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        };

        let rows = execute(&plan, &cache, &memtable).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows[0].fields.contains_key("cpu"));
        assert!(!rows[0].fields.contains_key("mem"));
    }
}
