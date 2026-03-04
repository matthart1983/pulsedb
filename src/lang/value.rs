use std::collections::BTreeMap;
use std::fmt;

use anyhow::{bail, Result};

/// Runtime value for the PulseLang interpreter.
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Str(String),
    Symbol(String),
    Timestamp(i64),
    Duration(i64),
    Null,

    // Compound
    IntVec(Vec<i64>),
    FloatVec(Vec<f64>),
    BoolVec(Vec<bool>),
    SymVec(Vec<String>),
    StrVec(Vec<String>),
    TimestampVec(Vec<i64>),

    List(Vec<Value>),
    Dict(BTreeMap<String, Value>),
    Table(Table),

    // Function
    Lambda {
        params: Vec<String>,
        body: crate::lang::ast::Expr,
    },
    BuiltinFn(String),
}

/// A columnar table: named columns of same-length vectors.
#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<String>,
    pub data: BTreeMap<String, Value>,
}

impl Value {
    /// Returns the type name as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "int",
            Value::UInt(_) => "uint",
            Value::Float(_) => "float",
            Value::Bool(_) => "bool",
            Value::Str(_) => "str",
            Value::Symbol(_) => "sym",
            Value::Timestamp(_) => "ts",
            Value::Duration(_) => "dur",
            Value::Null => "null",
            Value::IntVec(_) => "int[]",
            Value::FloatVec(_) => "float[]",
            Value::BoolVec(_) => "bool[]",
            Value::SymVec(_) => "sym[]",
            Value::StrVec(_) => "str[]",
            Value::TimestampVec(_) => "ts[]",
            Value::List(_) => "list",
            Value::Dict(_) => "dict",
            Value::Table(_) => "table",
            Value::Lambda { .. } => "fn",
            Value::BuiltinFn(_) => "fn",
        }
    }

    /// Returns the count/length of a value.
    pub fn count(&self) -> usize {
        match self {
            Value::IntVec(v) => v.len(),
            Value::FloatVec(v) => v.len(),
            Value::BoolVec(v) => v.len(),
            Value::SymVec(v) => v.len(),
            Value::StrVec(v) => v.len(),
            Value::TimestampVec(v) => v.len(),
            Value::List(v) => v.len(),
            Value::Str(s) => s.len(),
            Value::Table(t) => {
                t.data.values().next().map_or(0, |v| v.count())
            }
            _ => 1,
        }
    }

    /// Try to convert to a float vector for numeric operations.
    pub fn to_float_vec(&self) -> Result<Vec<f64>> {
        match self {
            Value::FloatVec(v) => Ok(v.clone()),
            Value::IntVec(v) => Ok(v.iter().map(|&x| x as f64).collect()),
            Value::BoolVec(v) => Ok(v.iter().map(|&b| if b { 1.0 } else { 0.0 }).collect()),
            Value::Float(f) => Ok(vec![*f]),
            Value::Int(i) => Ok(vec![*i as f64]),
            Value::Bool(b) => Ok(vec![if *b { 1.0 } else { 0.0 }]),
            _ => bail!("cannot convert {} to float vector", self.type_name()),
        }
    }

    /// Try to convert to an int vector.
    pub fn to_int_vec(&self) -> Result<Vec<i64>> {
        match self {
            Value::IntVec(v) => Ok(v.clone()),
            Value::BoolVec(v) => Ok(v.iter().map(|&b| if b { 1 } else { 0 }).collect()),
            Value::Int(i) => Ok(vec![*i]),
            Value::Bool(b) => Ok(vec![if *b { 1 } else { 0 }]),
            _ => bail!("cannot convert {} to int vector", self.type_name()),
        }
    }

    /// Try to extract a scalar float.
    pub fn as_float(&self) -> Result<f64> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            Value::UInt(u) => Ok(*u as f64),
            Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            _ => bail!("expected numeric, got {}", self.type_name()),
        }
    }

    /// Try to extract a scalar int.
    pub fn as_int(&self) -> Result<i64> {
        match self {
            Value::Int(i) => Ok(*i),
            Value::UInt(u) => Ok(*u as i64),
            Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
            _ => bail!("expected int, got {}", self.type_name()),
        }
    }

    /// Check if value is truthy.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0 && !f.is_nan(),
            Value::Null => false,
            _ => true,
        }
    }

    /// Promote scalars to match a vector length for element-wise ops.
    pub fn broadcast_float_pair(a: &Value, b: &Value) -> Result<(Vec<f64>, Vec<f64>)> {
        let va = a.to_float_vec()?;
        let vb = b.to_float_vec()?;
        if va.len() == vb.len() {
            Ok((va, vb))
        } else if va.len() == 1 {
            Ok((vec![va[0]; vb.len()], vb))
        } else if vb.len() == 1 {
            let len = va.len();
            Ok((va, vec![vb[0]; len]))
        } else {
            bail!("length mismatch: {} vs {}", va.len(), vb.len())
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::UInt(v) => write!(f, "{v}u"),
            Value::Float(v) => {
                if v.is_nan() {
                    write!(f, "0n")
                } else if v.is_infinite() {
                    if *v > 0.0 { write!(f, "0w") } else { write!(f, "-0w") }
                } else {
                    write!(f, "{v}")
                }
            }
            Value::Bool(b) => write!(f, "{}b", if *b { 1 } else { 0 }),
            Value::Str(s) => write!(f, "\"{s}\""),
            Value::Symbol(s) => write!(f, "`{s}"),
            Value::Timestamp(ns) => write!(f, "ts:{ns}"),
            Value::Duration(ns) => {
                let abs = ns.unsigned_abs();
                if abs % 604_800_000_000_000 == 0 {
                    write!(f, "{}w", ns / 604_800_000_000_000)
                } else if abs % 86_400_000_000_000 == 0 {
                    write!(f, "{}d", ns / 86_400_000_000_000)
                } else if abs % 3_600_000_000_000 == 0 {
                    write!(f, "{}h", ns / 3_600_000_000_000)
                } else if abs % 60_000_000_000 == 0 {
                    write!(f, "{}m", ns / 60_000_000_000)
                } else if abs % 1_000_000_000 == 0 {
                    write!(f, "{}s", ns / 1_000_000_000)
                } else if abs % 1_000_000 == 0 {
                    write!(f, "{}ms", ns / 1_000_000)
                } else if abs % 1_000 == 0 {
                    write!(f, "{}us", ns / 1_000)
                } else {
                    write!(f, "{ns}ns")
                }
            }
            Value::Null => write!(f, "0N"),
            Value::IntVec(v) => {
                let strs: Vec<String> = v.iter().map(|x| x.to_string()).collect();
                write!(f, "{}", strs.join(" "))
            }
            Value::FloatVec(v) => {
                let strs: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
                write!(f, "{}", strs.join(" "))
            }
            Value::BoolVec(v) => {
                let s: String = v.iter().map(|b| if *b { '1' } else { '0' }).collect();
                write!(f, "{s}b")
            }
            Value::SymVec(v) => {
                let strs: Vec<String> = v.iter().map(|s| format!("`{s}")).collect();
                write!(f, "{}", strs.join(""))
            }
            Value::StrVec(v) => {
                let strs: Vec<String> = v.iter().map(|s| format!("\"{s}\"")).collect();
                write!(f, "({})", strs.join("; "))
            }
            Value::TimestampVec(v) => {
                let strs: Vec<String> = v.iter().map(|x| format!("ts:{x}")).collect();
                write!(f, "{}", strs.join(" "))
            }
            Value::List(v) => {
                let strs: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
                write!(f, "({})", strs.join("; "))
            }
            Value::Dict(d) => {
                let entries: Vec<String> = d.iter().map(|(k, v)| format!("`{k}:{v}")).collect();
                write!(f, "{}", entries.join(" "))
            }
            Value::Table(t) => {
                write!(f, "+({})", t.columns.join("; "))
            }
            Value::Lambda { params, .. } => {
                write!(f, "{{[{}] ...}}", params.join(";"))
            }
            Value::BuiltinFn(name) => write!(f, "<builtin:{name}>"),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::UInt(a), Value::UInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::Symbol(a), Value::Symbol(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Duration(a), Value::Duration(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (Value::IntVec(a), Value::IntVec(b)) => a == b,
            (Value::FloatVec(a), Value::FloatVec(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| x == y || (x.is_nan() && y.is_nan()))
            }
            (Value::BoolVec(a), Value::BoolVec(b)) => a == b,
            (Value::SymVec(a), Value::SymVec(b)) => a == b,
            (Value::StrVec(a), Value::StrVec(b)) => a == b,
            (Value::TimestampVec(a), Value::TimestampVec(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            _ => false,
        }
    }
}
