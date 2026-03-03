//! Query planner — resolves a parsed SELECT statement into an executable plan.

use anyhow::Result;

use crate::index::InvertedIndex;
use crate::query::ast::*;
use crate::storage::SegmentCache;

/// A resolved query plan ready for execution.
#[derive(Debug)]
pub struct QueryPlan {
    pub measurement: String,
    pub fields: Vec<FieldExpr>,
    pub series_keys: Vec<String>,
    pub time_range: (i64, i64),
    pub group_by: Option<GroupBy>,
    pub fill: Option<FillPolicy>,
    pub order_desc: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Build a query plan from a parsed SELECT statement.
///
/// `memtable_keys` provides series keys from the active memtable so that
/// unflushed data is also discoverable by the planner.
pub fn plan_query(
    stmt: &SelectStatement,
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable_keys: &[String],
    now_ns: i64,
) -> Result<QueryPlan> {
    // 1. Collect all series keys for the measurement from segments + memtable.
    let mut all_keys = segment_cache.series_keys_for_measurement(&stmt.measurement);
    for key in memtable_keys {
        if key.starts_with(&stmt.measurement)
            && (key.len() == stmt.measurement.len()
                || key.as_bytes().get(stmt.measurement.len()) == Some(&b','))
            && !all_keys.contains(key)
        {
            all_keys.push(key.clone());
        }
    }

    // 2. Filter by tag predicates from the WHERE clause.
    let series_keys = if let Some(ref cond) = stmt.condition {
        filter_series_keys(&all_keys, cond, inverted_index)
    } else {
        all_keys
    };

    // 3. Resolve time range from the WHERE clause.
    let time_range = stmt
        .condition
        .as_ref()
        .map(|c| resolve_time_range(c, now_ns))
        .unwrap_or((i64::MIN, i64::MAX));

    let order_desc = stmt.order_by == Some(OrderBy::TimeDesc);

    Ok(QueryPlan {
        measurement: stmt.measurement.clone(),
        fields: stmt.fields.clone(),
        series_keys,
        time_range,
        group_by: stmt.group_by.clone(),
        fill: stmt.fill.clone(),
        order_desc,
        limit: stmt.limit,
        offset: stmt.offset,
    })
}

/// Filter series keys by evaluating tag predicates against the inverted index.
fn filter_series_keys(
    all_keys: &[String],
    clause: &WhereClause,
    index: &InvertedIndex,
) -> Vec<String> {
    let tag_ids = resolve_tag_predicate(clause, index);

    match tag_ids {
        // No tag predicates found — return all keys.
        None => all_keys.to_vec(),
        Some(_ids) => {
            // Convert SeriesIds back to matching keys by checking which keys
            // have tags that match. Since we don't have a reverse SeriesId→key
            // mapping, we filter by re-checking tag predicates against parsed
            // tags from the series key strings.
            all_keys
                .iter()
                .filter(|key| matches_tag_predicates(key, clause))
                .cloned()
                .collect()
        }
    }
}

/// Check whether a series key string matches the tag predicates in a WHERE clause.
fn matches_tag_predicates(series_key: &str, clause: &WhereClause) -> bool {
    match clause {
        WhereClause::Comparison { tag, op, value } => {
            let tags = parse_tags(series_key);
            if let Some(tag_val) = tags.get(tag.as_str()) {
                match op {
                    CompOp::Eq => tag_val == value,
                    CompOp::Neq => tag_val != value,
                    _ => true,
                }
            } else {
                matches!(op, CompOp::Neq)
            }
        }
        WhereClause::TimeComparison { .. } | WhereClause::TimeBetween { .. } => true,
        WhereClause::And(left, right) => {
            matches_tag_predicates(series_key, left)
                && matches_tag_predicates(series_key, right)
        }
        WhereClause::Or(left, right) => {
            matches_tag_predicates(series_key, left)
                || matches_tag_predicates(series_key, right)
        }
    }
}

/// Parse tags from a series key string like `measurement,tag1=val1,tag2=val2`.
fn parse_tags(series_key: &str) -> std::collections::BTreeMap<&str, &str> {
    let mut tags = std::collections::BTreeMap::new();
    for part in series_key.split(',').skip(1) {
        if let Some((k, v)) = part.split_once('=') {
            tags.insert(k, v);
        }
    }
    tags
}

/// Recursively resolve tag predicates to SeriesId sets.
/// Returns None if no tag predicates are present.
fn resolve_tag_predicate(
    clause: &WhereClause,
    index: &InvertedIndex,
) -> Option<Vec<crate::model::SeriesId>> {
    match clause {
        WhereClause::Comparison { tag, op, value } => {
            if *op == CompOp::Eq {
                let ids = index.lookup(tag, value);
                Some(ids.to_vec())
            } else {
                None
            }
        }
        WhereClause::TimeComparison { .. } | WhereClause::TimeBetween { .. } => None,
        WhereClause::And(left, right) => {
            let l = resolve_tag_predicate(left, index);
            let r = resolve_tag_predicate(right, index);
            match (l, r) {
                (Some(a), Some(b)) => {
                    Some(InvertedIndex::intersect(&[&a, &b]))
                }
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            }
        }
        WhereClause::Or(left, right) => {
            let l = resolve_tag_predicate(left, index);
            let r = resolve_tag_predicate(right, index);
            match (l, r) {
                (Some(a), Some(b)) => {
                    Some(InvertedIndex::union(&[&a, &b]))
                }
                // If one side has no tag predicate, we can't narrow down.
                _ => None,
            }
        }
    }
}

/// Walk the WHERE clause and extract a time range (min_ns, max_ns).
fn resolve_time_range(clause: &WhereClause, now_ns: i64) -> (i64, i64) {
    let mut min = i64::MIN;
    let mut max = i64::MAX;
    collect_time_bounds(clause, now_ns, &mut min, &mut max);
    (min, max)
}

fn collect_time_bounds(clause: &WhereClause, now_ns: i64, min: &mut i64, max: &mut i64) {
    match clause {
        WhereClause::TimeComparison { op, value } => {
            let ts = time_expr_to_nanos(value, now_ns);
            match op {
                CompOp::Gt => *min = (*min).max(ts + 1),
                CompOp::Gte => *min = (*min).max(ts),
                CompOp::Lt => *max = (*max).min(ts - 1),
                CompOp::Lte => *max = (*max).min(ts),
                CompOp::Eq => {
                    *min = (*min).max(ts);
                    *max = (*max).min(ts);
                }
                _ => {}
            }
        }
        WhereClause::TimeBetween { start, end } => {
            let s = time_expr_to_nanos(start, now_ns);
            let e = time_expr_to_nanos(end, now_ns);
            *min = (*min).max(s);
            *max = (*max).min(e);
        }
        WhereClause::And(left, right) => {
            collect_time_bounds(left, now_ns, min, max);
            collect_time_bounds(right, now_ns, min, max);
        }
        WhereClause::Or(_, _) => {
            // OR of time predicates — can't narrow, keep current bounds
        }
        WhereClause::Comparison { .. } => {}
    }
}

fn time_expr_to_nanos(expr: &TimeExpr, now_ns: i64) -> i64 {
    match expr {
        TimeExpr::Now => now_ns,
        TimeExpr::NowMinus(dur) => now_ns - dur.to_nanos() as i64,
        TimeExpr::Literal(ns) => *ns,
        TimeExpr::DateString(s) => {
            // Try to parse as an ISO date string
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                dt.and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                dt.timestamp_nanos_opt().unwrap_or(0)
            } else {
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::InvertedIndex;
    use crate::model::SeriesId;
    use crate::storage::{SegmentCache, SegmentMeta};
    use std::path::PathBuf;

    fn make_cache() -> SegmentCache {
        let mut cache = SegmentCache::new();
        cache.add(SegmentMeta {
            path: PathBuf::from("a.seg"),
            series_key: "cpu,host=web1".into(),
            min_time: 1000,
            max_time: 2000,
            point_count: 10,
        });
        cache.add(SegmentMeta {
            path: PathBuf::from("b.seg"),
            series_key: "cpu,host=web2".into(),
            min_time: 1000,
            max_time: 2000,
            point_count: 10,
        });
        cache.add(SegmentMeta {
            path: PathBuf::from("c.seg"),
            series_key: "mem,host=web1".into(),
            min_time: 1000,
            max_time: 2000,
            point_count: 10,
        });
        cache
    }

    fn make_index() -> InvertedIndex {
        let mut idx = InvertedIndex::new();
        let tags1: crate::model::Tags =
            [("host".into(), "web1".into())].into_iter().collect();
        let tags2: crate::model::Tags =
            [("host".into(), "web2".into())].into_iter().collect();
        idx.index_series(SeriesId(1), &tags1);
        idx.index_series(SeriesId(2), &tags2);
        idx
    }

    #[test]
    fn plan_no_where_returns_all_series_for_measurement() {
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Wildcard],
            measurement: "cpu".into(),
            condition: None,
            group_by: None,
            fill: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], 5000).unwrap();
        assert_eq!(plan.series_keys.len(), 2);
        assert!(plan.series_keys.contains(&"cpu,host=web1".to_string()));
        assert!(plan.series_keys.contains(&"cpu,host=web2".to_string()));
        assert_eq!(plan.time_range, (i64::MIN, i64::MAX));
    }

    #[test]
    fn plan_with_tag_filter() {
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Wildcard],
            measurement: "cpu".into(),
            condition: Some(WhereClause::Comparison {
                tag: "host".into(),
                op: CompOp::Eq,
                value: "web1".into(),
            }),
            group_by: None,
            fill: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], 5000).unwrap();
        assert_eq!(plan.series_keys, vec!["cpu,host=web1"]);
    }

    #[test]
    fn plan_with_time_range() {
        let now = 10_000i64;
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Wildcard],
            measurement: "cpu".into(),
            condition: Some(WhereClause::TimeComparison {
                op: CompOp::Gt,
                value: TimeExpr::NowMinus(Duration {
                    value: 5,
                    unit: DurationUnit::Seconds,
                }),
            }),
            group_by: None,
            fill: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], now).unwrap();
        let expected_min = now - 5_000_000_000 + 1;
        assert_eq!(plan.time_range.0, expected_min);
        assert_eq!(plan.time_range.1, i64::MAX);
    }

    #[test]
    fn plan_time_between() {
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Wildcard],
            measurement: "cpu".into(),
            condition: Some(WhereClause::TimeBetween {
                start: TimeExpr::Literal(100),
                end: TimeExpr::Literal(500),
            }),
            group_by: None,
            fill: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], 1000).unwrap();
        assert_eq!(plan.time_range, (100, 500));
    }

    #[test]
    fn plan_order_desc() {
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Wildcard],
            measurement: "cpu".into(),
            condition: None,
            group_by: None,
            fill: None,
            order_by: Some(OrderBy::TimeDesc),
            limit: Some(10),
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], 0).unwrap();
        assert!(plan.order_desc);
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn plan_combined_tag_and_time() {
        let stmt = SelectStatement {
            fields: vec![FieldExpr::Field("usage".into())],
            measurement: "cpu".into(),
            condition: Some(WhereClause::And(
                Box::new(WhereClause::Comparison {
                    tag: "host".into(),
                    op: CompOp::Eq,
                    value: "web1".into(),
                }),
                Box::new(WhereClause::TimeComparison {
                    op: CompOp::Gt,
                    value: TimeExpr::Literal(1500),
                }),
            )),
            group_by: None,
            fill: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let cache = make_cache();
        let index = make_index();
        let plan = plan_query(&stmt, &index, &cache, &[], 5000).unwrap();
        assert_eq!(plan.series_keys, vec!["cpu,host=web1"]);
        assert_eq!(plan.time_range.0, 1501);
    }
}
