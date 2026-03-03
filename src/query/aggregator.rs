//! Query aggregator — computes aggregations on scan results.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::model::FieldValue;
use crate::query::ast::*;
use crate::query::executor::ScanRow;
use crate::query::planner::QueryPlan;

/// A single row in the query result.
#[derive(Debug, Clone)]
pub struct ResultRow {
    pub timestamp: Option<i64>,
    pub tags: BTreeMap<String, String>,
    pub values: BTreeMap<String, f64>,
}

/// Query result returned to the client.
#[derive(Debug)]
pub struct QueryResult {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<ResultRow>,
}

/// Aggregate scan results according to the query plan.
pub fn aggregate(rows: Vec<ScanRow>, plan: &QueryPlan) -> Result<QueryResult> {
    let has_agg = plan.fields.iter().any(|f| matches!(f, FieldExpr::Aggregate { .. }));

    if !has_agg {
        return raw_result(rows, plan);
    }

    let group_by_tags = plan
        .group_by
        .as_ref()
        .map(|g| g.tags.clone())
        .unwrap_or_default();
    let time_interval = plan
        .group_by
        .as_ref()
        .and_then(|g| g.time_interval.as_ref())
        .map(|d| d.to_nanos() as i64);

    // Group rows by (time_bucket, tag_group_key).
    let mut groups: BTreeMap<(Option<i64>, BTreeMap<String, String>), Vec<&ScanRow>> =
        BTreeMap::new();

    for row in &rows {
        let bucket = time_interval.map(|interval| {
            if interval <= 0 {
                0
            } else {
                (row.timestamp / interval) * interval
            }
        });

        let mut tag_key = BTreeMap::new();
        for tag in &group_by_tags {
            let val = parse_tag_from_series_key(&row.series_key, tag)
                .unwrap_or_default();
            tag_key.insert(tag.clone(), val);
        }

        groups.entry((bucket, tag_key)).or_default().push(row);
    }

    // If GROUP BY time, fill missing buckets.
    if let Some(interval) = time_interval {
        fill_missing_buckets(&mut groups, plan, interval);
    }

    // If no rows at all, produce a single group so aggregations like count return 0.
    if groups.is_empty() {
        groups.insert((None, BTreeMap::new()), Vec::new());
    }

    // Compute aggregations for each group.
    let mut result_rows = Vec::new();
    for ((bucket, tags), group_rows) in &groups {
        let mut values = BTreeMap::new();
        for field_expr in &plan.fields {
            if let FieldExpr::Aggregate { func, field, alias } = field_expr {
                let col_name = alias
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| format_agg_name(func, field));
                let extracted: Vec<f64> = group_rows
                    .iter()
                    .filter_map(|r| r.fields.get(field).and_then(field_to_f64))
                    .collect();
                if let Some(val) = compute_agg(func, &extracted, group_rows, field) {
                    values.insert(col_name, val);
                }
            }
        }
        result_rows.push(ResultRow {
            timestamp: *bucket,
            tags: tags.clone(),
            values,
        });
    }

    let columns = build_columns(plan);

    Ok(QueryResult {
        name: plan.measurement.clone(),
        columns,
        rows: result_rows,
    })
}

/// Produce a raw (non-aggregated) result.
fn raw_result(rows: Vec<ScanRow>, plan: &QueryPlan) -> Result<QueryResult> {
    let mut columns = vec!["time".to_string()];
    let mut field_names: Vec<String> = Vec::new();

    for row in &rows {
        for key in row.fields.keys() {
            if !field_names.contains(key) {
                field_names.push(key.clone());
            }
        }
    }
    field_names.sort();
    columns.extend(field_names.iter().cloned());

    let result_rows = rows
        .into_iter()
        .map(|row| {
            let mut values = BTreeMap::new();
            for (name, val) in &row.fields {
                if let Some(f) = field_to_f64(val) {
                    values.insert(name.clone(), f);
                }
            }
            ResultRow {
                timestamp: Some(row.timestamp),
                tags: BTreeMap::new(),
                values,
            }
        })
        .collect();

    Ok(QueryResult {
        name: plan.measurement.clone(),
        columns,
        rows: result_rows,
    })
}

fn build_columns(plan: &QueryPlan) -> Vec<String> {
    let mut columns = vec!["time".to_string()];
    if let Some(ref gb) = plan.group_by {
        for tag in &gb.tags {
            columns.push(tag.clone());
        }
    }
    for field_expr in &plan.fields {
        if let FieldExpr::Aggregate { func, field, alias } = field_expr {
            let name = alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| format_agg_name(func, field));
            columns.push(name);
        }
    }
    columns
}

fn format_agg_name(func: &AggFunc, field: &str) -> String {
    let func_name = match func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Mean => "mean",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::First => "first",
        AggFunc::Last => "last",
        AggFunc::Stddev => "stddev",
        AggFunc::Percentile(p) => return format!("percentile({field}, {p})"),
    };
    format!("{func_name}({field})")
}

fn compute_agg(func: &AggFunc, values: &[f64], rows: &[&ScanRow], field: &str) -> Option<f64> {
    if values.is_empty() {
        return match func {
            AggFunc::Count => Some(0.0),
            _ => None,
        };
    }
    match func {
        AggFunc::Count => Some(values.len() as f64),
        AggFunc::Sum => Some(values.iter().sum()),
        AggFunc::Mean | AggFunc::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
        AggFunc::Min => values.iter().copied().reduce(f64::min),
        AggFunc::Max => values.iter().copied().reduce(f64::max),
        AggFunc::First => {
            rows.iter()
                .filter_map(|r| {
                    r.fields
                        .get(field)
                        .and_then(field_to_f64)
                        .map(|v| (r.timestamp, v))
                })
                .min_by_key(|(ts, _)| *ts)
                .map(|(_, v)| v)
        }
        AggFunc::Last => {
            rows.iter()
                .filter_map(|r| {
                    r.fields
                        .get(field)
                        .and_then(field_to_f64)
                        .map(|v| (r.timestamp, v))
                })
                .max_by_key(|(ts, _)| *ts)
                .map(|(_, v)| v)
        }
        AggFunc::Stddev => {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Some(variance.sqrt())
        }
        AggFunc::Percentile(p) => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
            let idx = idx.min(sorted.len() - 1);
            Some(sorted[idx])
        }
    }
}

fn field_to_f64(v: &FieldValue) -> Option<f64> {
    match v {
        FieldValue::Float(f) => Some(*f),
        FieldValue::Integer(i) => Some(*i as f64),
        FieldValue::UInteger(u) => Some(*u as f64),
        FieldValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        FieldValue::String(_) => None,
    }
}

/// Extract a tag value from a series key string.
fn parse_tag_from_series_key(series_key: &str, tag: &str) -> Option<String> {
    for part in series_key.split(',').skip(1) {
        if let Some((k, v)) = part.split_once('=') {
            if k == tag {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Fill missing time buckets for GROUP BY time queries.
fn fill_missing_buckets(
    groups: &mut BTreeMap<(Option<i64>, BTreeMap<String, String>), Vec<&ScanRow>>,
    plan: &QueryPlan,
    interval: i64,
) {
    if interval <= 0 || groups.is_empty() {
        return;
    }

    let fill = plan.fill.as_ref().unwrap_or(&FillPolicy::None);
    if matches!(fill, FillPolicy::None) {
        return;
    }

    // Collect all unique tag groups.
    let tag_groups: Vec<BTreeMap<String, String>> = groups
        .keys()
        .map(|(_, tags)| tags.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    // Find the overall time range from existing buckets.
    let min_bucket = groups
        .keys()
        .filter_map(|(b, _)| *b)
        .min()
        .unwrap_or(0);
    let max_bucket = groups
        .keys()
        .filter_map(|(b, _)| *b)
        .max()
        .unwrap_or(0);

    // Insert empty entries for missing buckets.
    for tag_group in &tag_groups {
        let mut bucket = min_bucket;
        while bucket <= max_bucket {
            let key = (Some(bucket), tag_group.clone());
            groups.entry(key).or_default();
            bucket += interval;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::ScanRow;
    use crate::query::planner::QueryPlan;

    fn make_scan_rows(timestamps: &[i64], field: &str, values: &[f64]) -> Vec<ScanRow> {
        timestamps
            .iter()
            .zip(values.iter())
            .map(|(&ts, &v)| ScanRow {
                timestamp: ts,
                series_key: "cpu,host=web1".into(),
                fields: [(field.into(), FieldValue::Float(v))].into_iter().collect(),
            })
            .collect()
    }

    fn plan_raw() -> QueryPlan {
        QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Field("usage".into())],
            series_keys: vec!["cpu,host=web1".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        }
    }

    fn plan_agg(func: AggFunc) -> QueryPlan {
        QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Aggregate {
                func,
                field: "usage".into(),
                alias: None,
            }],
            series_keys: vec!["cpu,host=web1".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        }
    }

    #[test]
    fn raw_query_passes_through() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[1.0, 2.0, 3.0]);
        let plan = plan_raw();
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].timestamp, Some(100));
        assert_eq!(result.rows[0].values["usage"], 1.0);
    }

    #[test]
    fn count_aggregation() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[1.0, 2.0, 3.0]);
        let plan = plan_agg(AggFunc::Count);
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values["count(usage)"], 3.0);
    }

    #[test]
    fn sum_aggregation() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[1.0, 2.0, 3.0]);
        let plan = plan_agg(AggFunc::Sum);
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows[0].values["sum(usage)"], 6.0);
    }

    #[test]
    fn mean_aggregation() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[1.0, 2.0, 3.0]);
        let plan = plan_agg(AggFunc::Mean);
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows[0].values["mean(usage)"], 2.0);
    }

    #[test]
    fn min_max_aggregation() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[5.0, 1.0, 9.0]);
        let plan_min = plan_agg(AggFunc::Min);
        let result = aggregate(rows.clone(), &plan_min).unwrap();
        assert_eq!(result.rows[0].values["min(usage)"], 1.0);

        let plan_max = plan_agg(AggFunc::Max);
        let result = aggregate(rows, &plan_max).unwrap();
        assert_eq!(result.rows[0].values["max(usage)"], 9.0);
    }

    #[test]
    fn first_last_aggregation() {
        let rows = make_scan_rows(&[300, 100, 200], "usage", &[30.0, 10.0, 20.0]);
        let plan_first = plan_agg(AggFunc::First);
        let result = aggregate(rows.clone(), &plan_first).unwrap();
        assert_eq!(result.rows[0].values["first(usage)"], 10.0);

        let plan_last = plan_agg(AggFunc::Last);
        let result = aggregate(rows, &plan_last).unwrap();
        assert_eq!(result.rows[0].values["last(usage)"], 30.0);
    }

    #[test]
    fn group_by_time_bucketing() {
        // Timestamps: 0, 5, 10, 15 with interval=10
        let rows = make_scan_rows(&[0, 5, 10, 15], "usage", &[1.0, 2.0, 3.0, 4.0]);
        let plan = QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Aggregate {
                func: AggFunc::Mean,
                field: "usage".into(),
                alias: None,
            }],
            series_keys: vec!["cpu,host=web1".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: Some(GroupBy {
                time_interval: Some(Duration {
                    value: 10,
                    unit: DurationUnit::Nanoseconds,
                }),
                tags: vec![],
            }),
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        };
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].timestamp, Some(0));
        assert_eq!(result.rows[0].values["mean(usage)"], 1.5);
        assert_eq!(result.rows[1].timestamp, Some(10));
        assert_eq!(result.rows[1].values["mean(usage)"], 3.5);
    }

    #[test]
    fn group_by_tag() {
        let mut rows = Vec::new();
        for &(ts, key, val) in &[
            (100i64, "cpu,host=web1", 1.0),
            (200, "cpu,host=web1", 2.0),
            (100, "cpu,host=web2", 10.0),
            (200, "cpu,host=web2", 20.0),
        ] {
            rows.push(ScanRow {
                timestamp: ts,
                series_key: key.into(),
                fields: [("usage".into(), FieldValue::Float(val))]
                    .into_iter()
                    .collect(),
            });
        }

        let plan = QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Aggregate {
                func: AggFunc::Sum,
                field: "usage".into(),
                alias: None,
            }],
            series_keys: vec!["cpu,host=web1".into(), "cpu,host=web2".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: Some(GroupBy {
                time_interval: None,
                tags: vec!["host".into()],
            }),
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        };

        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 2);

        let web1 = result
            .rows
            .iter()
            .find(|r| r.tags.get("host") == Some(&"web1".into()))
            .unwrap();
        assert_eq!(web1.values["sum(usage)"], 3.0);

        let web2 = result
            .rows
            .iter()
            .find(|r| r.tags.get("host") == Some(&"web2".into()))
            .unwrap();
        assert_eq!(web2.values["sum(usage)"], 30.0);
    }

    #[test]
    fn multiple_aggregations() {
        let rows = make_scan_rows(&[100, 200, 300], "usage", &[10.0, 20.0, 30.0]);
        let plan = QueryPlan {
            measurement: "cpu".into(),
            fields: vec![
                FieldExpr::Aggregate {
                    func: AggFunc::Min,
                    field: "usage".into(),
                    alias: None,
                },
                FieldExpr::Aggregate {
                    func: AggFunc::Max,
                    field: "usage".into(),
                    alias: None,
                },
                FieldExpr::Aggregate {
                    func: AggFunc::Mean,
                    field: "usage".into(),
                    alias: None,
                },
            ],
            series_keys: vec!["cpu,host=web1".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        };

        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values["min(usage)"], 10.0);
        assert_eq!(result.rows[0].values["max(usage)"], 30.0);
        assert_eq!(result.rows[0].values["mean(usage)"], 20.0);
    }

    #[test]
    fn empty_input() {
        let rows = Vec::new();
        let plan = plan_agg(AggFunc::Count);
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values["count(usage)"], 0.0);
    }

    #[test]
    fn aggregate_with_alias() {
        let rows = make_scan_rows(&[100, 200], "usage", &[5.0, 15.0]);
        let plan = QueryPlan {
            measurement: "cpu".into(),
            fields: vec![FieldExpr::Aggregate {
                func: AggFunc::Mean,
                field: "usage".into(),
                alias: Some("avg_usage".into()),
            }],
            series_keys: vec!["cpu,host=web1".into()],
            time_range: (i64::MIN, i64::MAX),
            group_by: None,
            fill: None,
            order_desc: false,
            limit: None,
            offset: None,
        };
        let result = aggregate(rows, &plan).unwrap();
        assert_eq!(result.rows[0].values["avg_usage"], 10.0);
    }

    #[test]
    fn stddev_aggregation() {
        let rows = make_scan_rows(&[1, 2, 3, 4, 5], "usage", &[2.0, 4.0, 4.0, 4.0, 6.0]);
        let plan = plan_agg(AggFunc::Stddev);
        let result = aggregate(rows, &plan).unwrap();
        let stddev = result.rows[0].values["stddev(usage)"];
        // mean = 4, variance = (4+0+0+0+4)/5 = 1.6, stddev ≈ 1.2649
        assert!((stddev - 1.2649).abs() < 0.001);
    }
}
