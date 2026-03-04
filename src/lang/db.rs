//! Database integration for PulseLang — resolves measurement names, columns,
//! tag filters, and time ranges against a live PulseDB engine.

use std::collections::BTreeMap;

use anyhow::{bail, Result};

use crate::index::InvertedIndex;
use crate::lang::ast::{Expr, TagCmpOp, TagPred};
use crate::lang::interpreter::{eval, Env};
use crate::lang::value::{Table, Value};
use crate::model::FieldValue;
use crate::query::ast::*;
use crate::query::executor::{execute, ScanRow};
use crate::query::planner::QueryPlan;
use crate::engine::memtable::MemTable;
use crate::storage::SegmentCache;

/// Evaluate a PulseLang expression with database context.
///
/// This wraps the interpreter, adding the ability to resolve measurement names,
/// tag filters, time ranges, and `select` expressions against live data.
pub fn eval_with_db(
    expr: &Expr,
    env: &mut Env,
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Value> {
    match expr {
        Expr::Member { object, field } => {
            // Try db resolution first: `cpu.usage_idle`
            if let Expr::Ident(measurement) = object.as_ref() {
                if is_measurement(measurement, segment_cache, memtable) {
                    // Check if we have a cached table for this measurement
                    let cache_key = format!("__scan_{measurement}");
                    if let Some(cached) = env.get(&cache_key) {
                        return eval_member_value(cached, field);
                    }
                    // Projection pushdown: for ts/time we use wildcard (timestamp
                    // comes from ScanRow regardless), otherwise scan only the
                    // requested field column.
                    if field == "ts" || field == "time" || field == "__timestamp" {
                        let rows = scan_measurement(
                            measurement,
                            None,
                            (i64::MIN, i64::MAX),
                            inverted_index,
                            segment_cache,
                            memtable,
                        )?;
                        return extract_column(&rows, field);
                    }
                    let rows = scan_measurement_column(
                        measurement,
                        field,
                        None,
                        (i64::MIN, i64::MAX),
                        inverted_index,
                        segment_cache,
                        memtable,
                    )?;
                    return extract_column(&rows, field);
                }
            }
            // Fall through to normal eval for non-measurement member access
            let obj = eval_with_db(object, env, inverted_index, segment_cache, memtable)?;
            eval_member_value(&obj, field)
        }

        Expr::Ident(name) => {
            // Check env first
            if env.get(name).is_some() {
                return Ok(env.get(name).unwrap().clone());
            }
            // Check scan cache
            let cache_key = format!("__scan_{name}");
            if let Some(cached) = env.get(&cache_key) {
                return Ok(cached.clone());
            }
            // Then check if it's a measurement name
            if is_measurement(name, segment_cache, memtable) {
                let rows = scan_measurement(
                    name,
                    None,
                    (i64::MIN, i64::MAX),
                    inverted_index,
                    segment_cache,
                    memtable,
                )?;
                let table = rows_to_table(name, &rows)?;
                env.set(cache_key, table.clone());
                return Ok(table);
            }
            // Normal eval
            eval(expr, env)
        }

        Expr::TagFilter { source, predicate } => {
            // Resolve the source measurement and apply tag filters
            let (measurement, existing_pred) = extract_measurement(source)?;
            let combined_pred = match existing_pred {
                Some(ep) => merge_predicates(ep, predicate),
                None => *predicate.clone(),
            };

            let where_clause = tag_pred_to_where(&combined_pred, env)?;

            // Projection pushdown: if source is a Member, only scan that column
            if let Expr::Member { field, .. } = source.as_ref() {
                let rows = scan_measurement_column(
                    &measurement,
                    field,
                    Some(&where_clause),
                    (i64::MIN, i64::MAX),
                    inverted_index,
                    segment_cache,
                    memtable,
                )?;
                return extract_column(&rows, field);
            }

            let rows = scan_measurement_filtered(
                &measurement,
                Some(&where_clause),
                (i64::MIN, i64::MAX),
                inverted_index,
                segment_cache,
                memtable,
            )?;

            rows_to_table(&measurement, &rows)
        }

        Expr::Within { source, start, end } => {
            let start_val = eval_with_db(start, env, inverted_index, segment_cache, memtable)?;
            let end_val = eval_with_db(end, env, inverted_index, segment_cache, memtable)?;
            let time_range = (value_to_nanos(&start_val)?, value_to_nanos(&end_val)?);

            match source.as_ref() {
                Expr::Ident(measurement) if is_measurement(measurement, segment_cache, memtable) => {
                    let rows = scan_measurement(
                        measurement,
                        None,
                        time_range,
                        inverted_index,
                        segment_cache,
                        memtable,
                    )?;
                    rows_to_table(measurement, &rows)
                }
                Expr::TagFilter { source: inner, predicate } => {
                    let (measurement, _) = extract_measurement(inner)?;
                    let where_clause = tag_pred_to_where(predicate, env)?;
                    let rows = scan_measurement_filtered(
                        &measurement,
                        Some(&where_clause),
                        time_range,
                        inverted_index,
                        segment_cache,
                        memtable,
                    )?;
                    rows_to_table(&measurement, &rows)
                }
                _ => {
                    // Eval source normally and just return it (within on non-db data is a no-op)
                    eval_with_db(source, env, inverted_index, segment_cache, memtable)
                }
            }
        }

        Expr::Select { fields, from, filter, by } => {
            let where_clause = match filter {
                Some(pred) => Some(tag_pred_to_where(pred, env)?),
                None => None,
            };
            let rows = scan_measurement_filtered(
                from,
                where_clause.as_ref(),
                (i64::MIN, i64::MAX),
                inverted_index,
                segment_cache,
                memtable,
            )?;

            // Build aggregated result
            eval_select(fields, from, &rows, by.as_deref(), env, inverted_index, segment_cache, memtable)
        }

        Expr::UnaryOp { op, operand } => {
            let v = eval_with_db(operand, env, inverted_index, segment_cache, memtable)?;
            crate::lang::interpreter::eval_unary_pub(*op, &v)
        }

        Expr::BinOp { op, left, right } => {
            let lv = eval_with_db(left, env, inverted_index, segment_cache, memtable)?;
            let rv = eval_with_db(right, env, inverted_index, segment_cache, memtable)?;
            crate::lang::interpreter::eval_binop_pub(*op, &lv, &rv)
        }

        Expr::Assign { name, value } => {
            let v = eval_with_db(value, env, inverted_index, segment_cache, memtable)?;
            env.set(name.clone(), v.clone());
            Ok(v)
        }

        Expr::Pipe { left, right } => {
            let lv = eval_with_db(left, env, inverted_index, segment_cache, memtable)?;
            let rv = eval_with_db(right, env, inverted_index, segment_cache, memtable)?;
            crate::lang::interpreter::eval_apply_pub(&rv, &[lv], env)
        }

        Expr::Apply { func, args } => {
            let fv = eval_with_db(func, env, inverted_index, segment_cache, memtable)?;
            let arg_vals: Result<Vec<Value>> = args
                .iter()
                .map(|a| eval_with_db(a, env, inverted_index, segment_cache, memtable))
                .collect();
            crate::lang::interpreter::eval_apply_pub(&fv, &arg_vals?, env)
        }

        Expr::Index { object, index } => {
            let obj = eval_with_db(object, env, inverted_index, segment_cache, memtable)?;
            let idx = eval_with_db(index, env, inverted_index, segment_cache, memtable)?;
            match &obj {
                Value::Lambda { .. } | Value::BuiltinFn(_) => {
                    crate::lang::interpreter::eval_apply_pub(&obj, &[idx], env)
                }
                _ => crate::lang::interpreter::eval_index_pub(&obj, &idx),
            }
        }

        // For all other expressions, delegate to the base interpreter
        _ => eval(expr, env),
    }
}

/// Check if a name is a known measurement in the database.
fn is_measurement(name: &str, segment_cache: &SegmentCache, memtable: &MemTable) -> bool {
    // Check segment cache
    let seg_keys = segment_cache.series_keys_for_measurement(name);
    if !seg_keys.is_empty() {
        return true;
    }
    // Check memtable
    for (key, _) in memtable.iter_series() {
        if key.starts_with(name)
            && (key.len() == name.len()
                || key.as_bytes().get(name.len()) == Some(&b','))
        {
            return true;
        }
    }
    false
}

/// Scan all data for a measurement.
fn scan_measurement(
    measurement: &str,
    condition: Option<&WhereClause>,
    _time_range: (i64, i64),
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Vec<ScanRow>> {
    let stmt = SelectStatement {
        fields: vec![FieldExpr::Wildcard],
        measurement: measurement.to_string(),
        condition: condition.cloned(),
        group_by: None,
        fill: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let memtable_keys: Vec<String> = memtable
        .iter_series()
        .map(|(k, _)| k.clone())
        .collect();

    let plan = crate::query::planner::plan_query(
        &stmt,
        inverted_index,
        segment_cache,
        &memtable_keys,
        now_ns,
    )?;

    execute(&plan, segment_cache, memtable)
}

/// Scan a single column from a measurement (projection pushdown).
fn scan_measurement_column(
    measurement: &str,
    field: &str,
    condition: Option<&WhereClause>,
    time_range: (i64, i64),
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Vec<ScanRow>> {
    let time_cond = if time_range != (i64::MIN, i64::MAX) {
        Some(WhereClause::TimeBetween {
            start: TimeExpr::Literal(time_range.0),
            end: TimeExpr::Literal(time_range.1),
        })
    } else {
        None
    };

    let combined = match (condition, time_cond) {
        (Some(tag), Some(time)) => Some(WhereClause::And(
            Box::new(tag.clone()),
            Box::new(time),
        )),
        (Some(tag), None) => Some(tag.clone()),
        (None, Some(time)) => Some(time),
        (None, None) => None,
    };

    let stmt = SelectStatement {
        fields: vec![FieldExpr::Field(field.to_string())],
        measurement: measurement.to_string(),
        condition: combined,
        group_by: None,
        fill: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let memtable_keys: Vec<String> = memtable
        .iter_series()
        .map(|(k, _)| k.clone())
        .collect();

    let plan = crate::query::planner::plan_query(
        &stmt,
        inverted_index,
        segment_cache,
        &memtable_keys,
        now_ns,
    )?;

    execute(&plan, segment_cache, memtable)
}

/// Scan with tag filter as WhereClause.
fn scan_measurement_filtered(
    measurement: &str,
    condition: Option<&WhereClause>,
    time_range: (i64, i64),
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Vec<ScanRow>> {
    let time_cond = if time_range != (i64::MIN, i64::MAX) {
        Some(WhereClause::TimeBetween {
            start: TimeExpr::Literal(time_range.0),
            end: TimeExpr::Literal(time_range.1),
        })
    } else {
        None
    };

    let combined = match (condition, time_cond) {
        (Some(tag), Some(time)) => Some(WhereClause::And(
            Box::new(tag.clone()),
            Box::new(time),
        )),
        (Some(tag), None) => Some(tag.clone()),
        (None, Some(time)) => Some(time),
        (None, None) => None,
    };

    scan_measurement(
        measurement,
        combined.as_ref(),
        time_range,
        inverted_index,
        segment_cache,
        memtable,
    )
}

/// Extract a column from scan rows as a typed vector.
fn extract_column(rows: &[ScanRow], field: &str) -> Result<Value> {
    if field == "ts" || field == "time" || field == "__timestamp" {
        let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp).collect();
        return Ok(Value::TimestampVec(timestamps));
    }

    let mut floats = Vec::new();
    let mut ints = Vec::new();
    let mut bools = Vec::new();
    let mut all_int = true;
    let mut all_bool = true;

    for row in rows {
        match row.fields.get(field) {
            Some(FieldValue::Float(v)) => {
                floats.push(*v);
                all_int = false;
                all_bool = false;
            }
            Some(FieldValue::Integer(v)) => {
                ints.push(*v);
                floats.push(*v as f64);
                all_bool = false;
            }
            Some(FieldValue::Boolean(v)) => {
                bools.push(*v);
                all_int = false;
            }
            _ => {
                floats.push(f64::NAN);
                all_int = false;
                all_bool = false;
            }
        }
    }

    if all_bool && !bools.is_empty() {
        Ok(Value::BoolVec(bools))
    } else if all_int && !ints.is_empty() {
        Ok(Value::IntVec(ints))
    } else {
        Ok(Value::FloatVec(floats))
    }
}

/// Convert scan rows to a PulseLang table value.
fn rows_to_table(_measurement: &str, rows: &[ScanRow]) -> Result<Value> {
    if rows.is_empty() {
        return Ok(Value::Table(Table {
            columns: vec!["ts".into()],
            data: BTreeMap::from([("ts".into(), Value::TimestampVec(vec![]))]),
        }));
    }

    // Collect all field names
    let mut field_names: Vec<String> = Vec::new();
    for row in rows {
        for key in row.fields.keys() {
            if !field_names.contains(key) {
                field_names.push(key.clone());
            }
        }
    }
    field_names.sort();

    let mut columns = vec!["ts".into()];
    columns.extend(field_names.iter().cloned());

    let mut data: BTreeMap<String, Value> = BTreeMap::new();
    data.insert(
        "ts".into(),
        Value::TimestampVec(rows.iter().map(|r| r.timestamp).collect()),
    );

    for name in &field_names {
        let col = extract_column(rows, name)?;
        data.insert(name.clone(), col);
    }

    Ok(Value::Table(Table { columns, data }))
}

/// Evaluate a PulseLang `select` expression against scanned rows.
fn eval_select(
    fields: &[crate::lang::ast::SelectField],
    measurement: &str,
    rows: &[ScanRow],
    by: Option<&Expr>,
    env: &mut Env,
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Value> {
    // Convert PulseLang select fields to PulseQL FieldExprs for the aggregator
    let mut pulseql_fields = Vec::new();
    for f in fields {
        if let Some(ref func_name) = f.func {
            let agg_func = match func_name.as_str() {
                "count" => AggFunc::Count,
                "sum" => AggFunc::Sum,
                "avg" | "mean" => AggFunc::Mean,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                "first" => AggFunc::First,
                "last" => AggFunc::Last,
                "dev" | "stddev" => AggFunc::Stddev,
                _ => bail!("unknown aggregation function: {func_name}"),
            };
            pulseql_fields.push(FieldExpr::Aggregate {
                func: agg_func,
                field: f.field.clone(),
                alias: f.alias.clone(),
            });
        } else {
            pulseql_fields.push(FieldExpr::Field(f.field.clone()));
        }
    }

    // Resolve GROUP BY
    let group_by = if let Some(by_expr) = by {
        // Evaluate the by expression to determine bucketing
        let by_val = eval_with_db(by_expr, env, inverted_index, segment_cache, memtable)?;
        match by_val {
            Value::Duration(ns) => Some(GroupBy {
                time_interval: Some(crate::query::ast::Duration {
                    value: ns as u64,
                    unit: DurationUnit::Nanoseconds,
                }),
                tags: vec![],
            }),
            _ => None,
        }
    } else {
        None
    };

    let plan = QueryPlan {
        measurement: measurement.to_string(),
        fields: pulseql_fields,
        series_keys: rows.iter().map(|r| r.series_key.clone()).collect::<Vec<_>>(),
        time_range: (i64::MIN, i64::MAX),
        group_by,
        fill: None,
        order_desc: false,
        limit: None,
        offset: None,
    };

    // Deduplicate series keys
    let mut unique_keys: Vec<String> = Vec::new();
    for k in &plan.series_keys {
        if !unique_keys.contains(k) {
            unique_keys.push(k.clone());
        }
    }

    let plan = QueryPlan {
        series_keys: unique_keys,
        ..plan
    };

    let result = crate::query::aggregator::aggregate(rows.to_vec(), &plan)?;

    // Convert QueryResult to a PulseLang table
    let mut columns = result.columns.clone();
    let mut data: BTreeMap<String, Value> = BTreeMap::new();

    // Time column
    let timestamps: Vec<i64> = result.rows.iter().filter_map(|r| r.timestamp).collect();
    if !timestamps.is_empty() {
        data.insert("ts".into(), Value::TimestampVec(timestamps));
    }

    // Value columns
    for col in &result.columns {
        if col == "time" {
            continue;
        }
        let vals: Vec<f64> = result.rows.iter().filter_map(|r| r.values.get(col).copied()).collect();
        if !vals.is_empty() {
            data.insert(col.clone(), Value::FloatVec(vals));
        }
    }

    // Replace "time" with "ts" in columns
    columns = columns.into_iter().map(|c| if c == "time" { "ts".into() } else { c }).collect();

    Ok(Value::Table(Table { columns, data }))
}

/// Convert a PulseLang TagPred to a PulseQL WhereClause.
fn tag_pred_to_where(pred: &TagPred, env: &mut Env) -> Result<WhereClause> {
    match pred {
        TagPred::Cmp { tag, op, value } => {
            let val_str = match eval(value, env)? {
                Value::Symbol(s) => s,
                Value::Str(s) => s,
                other => format!("{other}"),
            };
            let comp_op = match op {
                TagCmpOp::Eq => CompOp::Eq,
                TagCmpOp::Neq => CompOp::Neq,
                TagCmpOp::Like => CompOp::RegexMatch,
                TagCmpOp::In => CompOp::Eq, // simplified
            };
            Ok(WhereClause::Comparison {
                tag: tag.clone(),
                op: comp_op,
                value: val_str,
            })
        }
        TagPred::And(a, b) => {
            let left = tag_pred_to_where(a, env)?;
            let right = tag_pred_to_where(b, env)?;
            Ok(WhereClause::And(Box::new(left), Box::new(right)))
        }
        TagPred::Or(a, b) => {
            let left = tag_pred_to_where(a, env)?;
            let right = tag_pred_to_where(b, env)?;
            Ok(WhereClause::Or(Box::new(left), Box::new(right)))
        }
    }
}

/// Extract measurement name from nested expression tree.
fn extract_measurement(expr: &Expr) -> Result<(String, Option<TagPred>)> {
    match expr {
        Expr::Ident(name) => Ok((name.clone(), None)),
        Expr::Member { object, .. } => extract_measurement(object),
        Expr::TagFilter { source, predicate } => {
            let (measurement, existing) = extract_measurement(source)?;
            let combined = match existing {
                Some(ep) => merge_predicates(ep, predicate),
                None => *predicate.clone(),
            };
            Ok((measurement, Some(combined)))
        }
        _ => bail!("expected measurement name"),
    }
}

fn merge_predicates(a: TagPred, b: &TagPred) -> TagPred {
    TagPred::And(Box::new(a), Box::new(b.clone()))
}

fn value_to_nanos(val: &Value) -> Result<i64> {
    match val {
        Value::Timestamp(ns) => Ok(*ns),
        Value::Duration(ns) => Ok(*ns),
        Value::Int(v) => Ok(*v),
        _ => bail!("expected timestamp or duration, got {}", val.type_name()),
    }
}

fn eval_member_value(obj: &Value, field: &str) -> Result<Value> {
    crate::lang::interpreter::eval_member_pub(obj, field)
}

/// Convenience: evaluate a PulseLang string with database context.
pub fn eval_str_with_db(
    input: &str,
    env: &mut Env,
    inverted_index: &InvertedIndex,
    segment_cache: &SegmentCache,
    memtable: &MemTable,
) -> Result<Value> {
    let parser = crate::lang::parser::Parser::new(input)?;
    let expr = parser.parse()?;
    eval_with_db(&expr, env, inverted_index, segment_cache, memtable)
}


