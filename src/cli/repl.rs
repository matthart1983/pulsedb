use std::path::Path;

use anyhow::Result;

use crate::engine::Database;
use crate::lang::interpreter::Env;
use crate::lang::value::Value;

/// Output format for REPL results.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s {
            "json" => OutputFormat::Json,
            "csv" => OutputFormat::Csv,
            _ => OutputFormat::Text,
        }
    }
}

/// Format a Value for display in the given output format.
pub fn format_value(value: &Value, format: OutputFormat) -> String {
    match format {
        OutputFormat::Text => format_text(value),
        OutputFormat::Json => format_json(value),
        OutputFormat::Csv => format_csv(value),
    }
}

fn format_text(value: &Value) -> String {
    if let Value::Table(table) = value {
        let num_rows = table
            .data
            .values()
            .next()
            .map_or(0, |v| v.count());

        // Compute column widths
        let mut col_widths: Vec<usize> = table.columns.iter().map(|c| c.len()).collect();
        let mut col_strs: Vec<Vec<String>> = Vec::new();

        for (ci, col_name) in table.columns.iter().enumerate() {
            let mut cells = Vec::new();
            if let Some(col_val) = table.data.get(col_name) {
                for ri in 0..num_rows {
                    let cell = format_cell(col_val, ri);
                    if cell.len() > col_widths[ci] {
                        col_widths[ci] = cell.len();
                    }
                    cells.push(cell);
                }
            }
            col_strs.push(cells);
        }

        let mut out = String::new();

        // Header
        let header: Vec<String> = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<width$}", c, width = col_widths[i]))
            .collect();
        out.push_str(&header.join(" | "));
        out.push('\n');

        // Separator
        let sep: Vec<String> = col_widths.iter().map(|w| "-".repeat(*w)).collect();
        out.push_str(&sep.join("-|-"));
        out.push('\n');

        // Rows
        for ri in 0..num_rows {
            let row: Vec<String> = table
                .columns
                .iter()
                .enumerate()
                .map(|(ci, _)| {
                    let cell = col_strs.get(ci).and_then(|c| c.get(ri)).cloned().unwrap_or_default();
                    format!("{:<width$}", cell, width = col_widths[ci])
                })
                .collect();
            out.push_str(&row.join(" | "));
            out.push('\n');
        }

        out.trim_end().to_string()
    } else {
        format!("{value}")
    }
}

fn format_cell(col_val: &Value, index: usize) -> String {
    match col_val {
        Value::IntVec(v) => v.get(index).map_or(String::new(), |x| x.to_string()),
        Value::FloatVec(v) => v.get(index).map_or(String::new(), |x| format!("{x}")),
        Value::BoolVec(v) => v.get(index).map_or(String::new(), |b| format!("{}b", if *b { 1 } else { 0 })),
        Value::SymVec(v) => v.get(index).map_or(String::new(), |s| format!("`{s}")),
        Value::StrVec(v) => v.get(index).map_or(String::new(), |s| format!("\"{s}\"")),
        Value::TimestampVec(v) => v.get(index).map_or(String::new(), |x| x.to_string()),
        _ => format!("{col_val}"),
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Int(v) => serde_json::Value::Number((*v).into()),
        Value::UInt(v) => serde_json::Value::Number((*v).into()),
        Value::Float(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::Symbol(s) => serde_json::Value::String(format!("`{s}")),
        Value::Timestamp(ns) => serde_json::Value::Number((*ns).into()),
        Value::Duration(ns) => serde_json::Value::Number((*ns).into()),
        Value::Null => serde_json::Value::Null,
        Value::IntVec(v) => serde_json::Value::Array(
            v.iter().map(|x| serde_json::Value::Number((*x).into())).collect(),
        ),
        Value::FloatVec(v) => serde_json::Value::Array(
            v.iter()
                .map(|x| {
                    serde_json::Number::from_f64(*x)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::BoolVec(v) => serde_json::Value::Array(
            v.iter().map(|b| serde_json::Value::Bool(*b)).collect(),
        ),
        Value::SymVec(v) => serde_json::Value::Array(
            v.iter().map(|s| serde_json::Value::String(s.clone())).collect(),
        ),
        Value::StrVec(v) => serde_json::Value::Array(
            v.iter().map(|s| serde_json::Value::String(s.clone())).collect(),
        ),
        Value::TimestampVec(v) => serde_json::Value::Array(
            v.iter().map(|x| serde_json::Value::Number((*x).into())).collect(),
        ),
        Value::List(v) => serde_json::Value::Array(
            v.iter().map(value_to_json).collect(),
        ),
        Value::Dict(d) => {
            let map: serde_json::Map<String, serde_json::Value> = d
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Value::Table(table) => {
            let num_rows = table.data.values().next().map_or(0, |v| v.count());
            let mut rows = Vec::new();
            for ri in 0..num_rows {
                let mut row = serde_json::Map::new();
                for col_name in &table.columns {
                    if let Some(col_val) = table.data.get(col_name) {
                        row.insert(col_name.clone(), json_cell(col_val, ri));
                    }
                }
                rows.push(serde_json::Value::Object(row));
            }
            serde_json::Value::Array(rows)
        }
        Value::Lambda { params, .. } => {
            serde_json::Value::String(format!("{{[{}] ...}}", params.join(";")))
        }
        Value::BuiltinFn(name) => serde_json::Value::String(format!("<builtin:{name}>")),
    }
}

fn json_cell(col_val: &Value, index: usize) -> serde_json::Value {
    match col_val {
        Value::IntVec(v) => v
            .get(index)
            .map(|x| serde_json::Value::Number((*x).into()))
            .unwrap_or(serde_json::Value::Null),
        Value::FloatVec(v) => v
            .get(index)
            .and_then(|x| serde_json::Number::from_f64(*x).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        Value::BoolVec(v) => v
            .get(index)
            .map(|b| serde_json::Value::Bool(*b))
            .unwrap_or(serde_json::Value::Null),
        Value::SymVec(v) => v
            .get(index)
            .map(|s| serde_json::Value::String(s.clone()))
            .unwrap_or(serde_json::Value::Null),
        Value::StrVec(v) => v
            .get(index)
            .map(|s| serde_json::Value::String(s.clone()))
            .unwrap_or(serde_json::Value::Null),
        Value::TimestampVec(v) => v
            .get(index)
            .map(|x| serde_json::Value::Number((*x).into()))
            .unwrap_or(serde_json::Value::Null),
        _ => value_to_json(col_val),
    }
}

fn format_json(value: &Value) -> String {
    let json = value_to_json(value);
    serde_json::to_string_pretty(&json).unwrap_or_else(|_| format!("{value}"))
}

fn format_csv(value: &Value) -> String {
    match value {
        Value::Table(table) => {
            let num_rows = table.data.values().next().map_or(0, |v| v.count());
            let mut out = table.columns.join(",");
            out.push('\n');
            for ri in 0..num_rows {
                let row: Vec<String> = table
                    .columns
                    .iter()
                    .map(|col_name| {
                        table
                            .data
                            .get(col_name)
                            .map(|v| format_cell(v, ri))
                            .unwrap_or_default()
                    })
                    .collect();
                out.push_str(&row.join(","));
                out.push('\n');
            }
            out.trim_end().to_string()
        }
        Value::IntVec(v) => v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("\n"),
        Value::FloatVec(v) => v.iter().map(|x| format!("{x}")).collect::<Vec<_>>().join("\n"),
        Value::BoolVec(v) => v.iter().map(|b| format!("{b}")).collect::<Vec<_>>().join("\n"),
        Value::SymVec(v) | Value::StrVec(v) => v.join("\n"),
        Value::TimestampVec(v) => v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("\n"),
        Value::List(v) => v.iter().map(|x| format!("{x}")).collect::<Vec<_>>().join("\n"),
        _ => format!("{value}"),
    }
}

/// Run the interactive REPL.
pub fn run_repl(db: &Database, format: OutputFormat) -> Result<()> {
    let mut rl = rustyline::DefaultEditor::new()?;
    let mut env = Env::new();
    let mut fmt = format;

    println!(
        "PulseLang v{} — PulseDB interactive shell",
        env!("CARGO_PKG_VERSION")
    );
    println!("Type expressions to evaluate. Ctrl-D to exit.");

    loop {
        match rl.readline("pulse> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                if trimmed == "\\q" || trimmed == "exit" {
                    println!("Bye!");
                    break;
                }

                if trimmed.starts_with("\\fmt ") {
                    let arg = trimmed.strip_prefix("\\fmt ").unwrap().trim();
                    fmt = OutputFormat::from_str(arg);
                    println!("Output format: {arg}");
                    continue;
                }

                let is_assign = trimmed.contains(':')
                    && !trimmed.starts_with('"')
                    && !trimmed.starts_with('$');

                match db.query_lang_with_env(trimmed, &mut env) {
                    Ok(ref val) if matches!(val, Value::Null) && is_assign => {}
                    Ok(val) => {
                        println!("{}", format_value(&val, fmt));
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        }
    }

    Ok(())
}

/// Execute a single expression.
pub fn run_expression(db: &Database, expr: &str, format: OutputFormat) -> Result<()> {
    let result = db.query_lang(expr)?;
    println!("{}", format_value(&result, format));
    Ok(())
}

/// Execute a .pulse script file.
pub fn run_script(db: &Database, path: &Path, format: OutputFormat) -> Result<()> {
    let contents = std::fs::read_to_string(path)?;
    let mut env = Env::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('/') {
            continue;
        }

        match db.query_lang_with_env(trimmed, &mut env) {
            Ok(val) => {
                if !matches!(val, Value::Null) {
                    println!("{}", format_value(&val, format));
                }
            }
            Err(e) => {
                eprintln!("error: {e}");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::lang::value::Table;

    #[test]
    fn format_text_scalar() {
        assert_eq!(format_value(&Value::Int(42), OutputFormat::Text), "42");
        assert_eq!(format_value(&Value::Float(3.14), OutputFormat::Text), "3.14");
    }

    #[test]
    fn format_text_vector() {
        let v = Value::IntVec(vec![1, 2, 3]);
        assert_eq!(format_value(&v, OutputFormat::Text), "1 2 3");
    }

    #[test]
    fn format_text_table() {
        let table = Value::Table(Table {
            columns: vec!["a".into(), "b".into()],
            data: BTreeMap::from([
                ("a".into(), Value::IntVec(vec![1, 2])),
                ("b".into(), Value::FloatVec(vec![3.0, 4.5])),
            ]),
        });
        let out = format_value(&table, OutputFormat::Text);
        assert!(out.contains("a"));
        assert!(out.contains("b"));
        assert!(out.contains("1"));
        assert!(out.contains("4.5"));
    }

    #[test]
    fn format_json_scalar() {
        assert_eq!(format_value(&Value::Int(42), OutputFormat::Json), "42");
    }

    #[test]
    fn format_json_table() {
        let table = Value::Table(Table {
            columns: vec!["x".into()],
            data: BTreeMap::from([("x".into(), Value::IntVec(vec![10]))]),
        });
        let out = format_value(&table, OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed[0]["x"], 10);
    }

    #[test]
    fn format_csv_table() {
        let table = Value::Table(Table {
            columns: vec!["a".into(), "b".into()],
            data: BTreeMap::from([
                ("a".into(), Value::IntVec(vec![1, 2])),
                ("b".into(), Value::IntVec(vec![3, 4])),
            ]),
        });
        let out = format_value(&table, OutputFormat::Csv);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "a,b");
        assert_eq!(lines[1], "1,3");
        assert_eq!(lines[2], "2,4");
    }

    #[test]
    fn format_csv_vector() {
        let v = Value::FloatVec(vec![1.0, 2.5, 3.0]);
        let out = format_value(&v, OutputFormat::Csv);
        assert_eq!(out, "1\n2.5\n3");
    }

    #[test]
    fn error_includes_position() {
        let err = crate::lang::parser::Parser::new("42 + @").unwrap().parse();
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("line 1"), "error should include line: {msg}");
        assert!(msg.contains("col"), "error should include col: {msg}");
    }
}
