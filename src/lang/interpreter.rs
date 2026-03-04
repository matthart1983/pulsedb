use std::collections::BTreeMap;

use anyhow::{bail, Result};

use crate::lang::ast::*;
use crate::lang::value::{Table, Value};
use crate::query::ast::DurationUnit;

/// Environment: variable bindings for the interpreter.
#[derive(Debug, Clone)]
pub struct Env {
    scopes: Vec<BTreeMap<String, Value>>,
}

impl Env {
    pub fn new() -> Self {
        let mut global = BTreeMap::new();
        // Register multi-arg builtins
        for name in &[
            "mavg", "msum", "mmin", "mmax", "mdev", "mcount",
            "ema", "wma", "wavg", "xbar", "resample", "asof", "pct", "cor",
        ] {
            global.insert(name.to_string(), Value::BuiltinFn(name.to_string()));
        }
        // Register monadic builtins as values too (for pipeline usage)
        for name in &[
            "neg", "not", "abs", "sqrt", "exp", "log", "ceil", "floor",
            "signum", "reciprocal",
            "sum", "avg", "mean", "min", "max", "count", "first", "last",
            "med", "dev", "var",
            "sums", "avgs", "mins", "maxs", "prds",
            "til", "rev", "asc", "desc", "distinct", "group", "flip", "raze",
            "where",
            "deltas", "ratios", "prev", "next", "fills", "ffill", "bfill",
            "type", "null", "key", "value", "string",
            "upper", "lower", "trim",
        ] {
            global.insert(name.to_string(), Value::BuiltinFn(name.to_string()));
        }
        Env {
            scopes: vec![global],
        }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(name) {
                return Some(v);
            }
        }
        None
    }

    pub fn set(&mut self, name: String, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name, value);
        }
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(BTreeMap::new());
    }

    pub fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }
}

impl Default for Env {
    fn default() -> Self {
        Self::new()
    }
}

/// Tree-walk interpreter for PulseLang expressions.
pub fn eval(expr: &Expr, env: &mut Env) -> Result<Value> {
    match expr {
        Expr::Int(v) => Ok(Value::Int(*v)),
        Expr::UInt(v) => Ok(Value::UInt(*v)),
        Expr::Float(v) => Ok(Value::Float(*v)),
        Expr::Bool(v) => Ok(Value::Bool(*v)),
        Expr::Str(s) => Ok(Value::Str(s.clone())),
        Expr::Symbol(s) => Ok(Value::Symbol(s.clone())),
        Expr::Timestamp(s) => Ok(Value::Timestamp(parse_timestamp(s)?)),
        Expr::Duration(v, u) => Ok(Value::Duration(duration_to_nanos(*v, *u))),
        Expr::Null(_) => Ok(Value::Null),

        Expr::Ident(name) => {
            if let Some(v) = env.get(name) {
                Ok(v.clone())
            } else {
                bail!("undefined variable: {name}")
            }
        }

        Expr::Vec(exprs) => eval_vec(exprs, env),
        Expr::BoolVec(bools) => Ok(Value::BoolVec(bools.clone())),

        Expr::List(exprs) => {
            let vals: Result<Vec<Value>> = exprs.iter().map(|e| eval(e, env)).collect();
            Ok(Value::List(vals?))
        }

        Expr::Dict { keys, values } => {
            let k = eval(keys, env)?;
            let v = eval(values, env)?;
            eval_dict(k, v)
        }

        Expr::Table(cols) => {
            let mut columns = Vec::new();
            let mut data = BTreeMap::new();
            for (name, expr) in cols {
                columns.push(name.clone());
                data.insert(name.clone(), eval(expr, env)?);
            }
            Ok(Value::Table(Table { columns, data }))
        }

        Expr::Lambda { params, body } => Ok(Value::Lambda {
            params: params.clone(),
            body: *body.clone(),
        }),

        Expr::Assign { name, value } => {
            let v = eval(value, env)?;
            env.set(name.clone(), v.clone());
            Ok(v)
        }

        Expr::BinOp { op, left, right } => {
            let lv = eval(left, env)?;
            let rv = eval(right, env)?;
            eval_binop(*op, &lv, &rv)
        }

        Expr::UnaryOp { op, operand } => {
            let v = eval(operand, env)?;
            eval_unary(*op, &v)
        }

        Expr::Apply { func, args } => {
            let fv = eval(func, env)?;
            let arg_vals: Result<Vec<Value>> = args.iter().map(|a| eval(a, env)).collect();
            let arg_vals = arg_vals?;
            eval_apply(&fv, &arg_vals, env)
        }

        Expr::Member { object, field } => {
            let obj = eval(object, env)?;
            eval_member(&obj, field)
        }

        Expr::Index { object, index } => {
            let obj = eval(object, env)?;
            let idx = eval(index, env)?;
            // If obj is callable, treat x[y] as function application
            match &obj {
                Value::Lambda { .. } | Value::BuiltinFn(_) => {
                    eval_apply(&obj, &[idx], env)
                }
                _ => eval_index(&obj, &idx),
            }
        }

        Expr::Pipe { left, right } => {
            let lv = eval(left, env)?;
            // Apply right as a function to left
            let rv = eval(right, env)?;
            eval_apply(&rv, &[lv], env)
        }

        Expr::Cond { pairs, default } => {
            for (cond, then) in pairs {
                let cv = eval(cond, env)?;
                if cv.is_truthy() {
                    return eval(then, env);
                }
            }
            eval(default, env)
        }

        Expr::Iterator { func, iter, arg } => {
            let fv = eval(func, env)?;
            let av = eval(arg, env)?;
            eval_iterator(&fv, *iter, &av, env)
        }

        Expr::Block(exprs) => {
            let mut result = Value::Null;
            for e in exprs {
                result = eval(e, env)?;
            }
            Ok(result)
        }

        // Database-dependent features return placeholders for now
        Expr::TagFilter { source, .. } => {
            eval(source, env)
        }
        Expr::Within { source, .. } => {
            eval(source, env)
        }
        Expr::Select { .. } => {
            bail!("select expressions require database connection")
        }
    }
}

fn eval_dict(keys: Value, values: Value) -> Result<Value> {
    match (keys, values) {
        (Value::SymVec(ks), Value::IntVec(vs)) => {
            let mut d = BTreeMap::new();
            for (k, v) in ks.into_iter().zip(vs) {
                d.insert(k, Value::Int(v));
            }
            Ok(Value::Dict(d))
        }
        (Value::SymVec(ks), Value::FloatVec(vs)) => {
            let mut d = BTreeMap::new();
            for (k, v) in ks.into_iter().zip(vs) {
                d.insert(k, Value::Float(v));
            }
            Ok(Value::Dict(d))
        }
        (Value::SymVec(ks), Value::List(vs)) => {
            let mut d = BTreeMap::new();
            for (k, v) in ks.into_iter().zip(vs) {
                d.insert(k, v);
            }
            Ok(Value::Dict(d))
        }
        (Value::SymVec(ks), Value::SymVec(vs)) => {
            let mut d = BTreeMap::new();
            for (k, v) in ks.into_iter().zip(vs) {
                d.insert(k, Value::Symbol(v));
            }
            Ok(Value::Dict(d))
        }
        (k, v) => bail!("cannot create dict from {} and {}", k.type_name(), v.type_name()),
    }
}

fn eval_vec(exprs: &[Expr], env: &mut Env) -> Result<Value> {
    let vals: Result<Vec<Value>> = exprs.iter().map(|e| eval(e, env)).collect();
    let vals = vals?;
    // Try to produce a typed vector
    if vals.iter().all(|v| matches!(v, Value::Int(_))) {
        Ok(Value::IntVec(
            vals.into_iter()
                .map(|v| match v { Value::Int(i) => i, _ => unreachable!() })
                .collect(),
        ))
    } else if vals.iter().all(|v| matches!(v, Value::Float(_))) {
        Ok(Value::FloatVec(
            vals.into_iter()
                .map(|v| match v { Value::Float(f) => f, _ => unreachable!() })
                .collect(),
        ))
    } else if vals.iter().all(|v| matches!(v, Value::Symbol(_))) {
        Ok(Value::SymVec(
            vals.into_iter()
                .map(|v| match v { Value::Symbol(s) => s, _ => unreachable!() })
                .collect(),
        ))
    } else if vals.iter().all(|v| matches!(v, Value::Int(_) | Value::Float(_))) {
        // Promote to float
        Ok(Value::FloatVec(
            vals.into_iter()
                .map(|v| match v {
                    Value::Float(f) => f,
                    Value::Int(i) => i as f64,
                    _ => unreachable!(),
                })
                .collect(),
        ))
    } else {
        Ok(Value::List(vals))
    }
}

fn eval_binop(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Pow | BinOp::Mod => {
            eval_arith(op, left, right)
        }
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => {
            eval_comparison(op, left, right)
        }
        BinOp::And => {
            let (a, b) = Value::broadcast_float_pair(left, right)?;
            let result: Vec<bool> = a.iter().zip(b.iter()).map(|(x, y)| *x != 0.0 && *y != 0.0).collect();
            if result.len() == 1 { Ok(Value::Bool(result[0])) } else { Ok(Value::BoolVec(result)) }
        }
        BinOp::Or => {
            let (a, b) = Value::broadcast_float_pair(left, right)?;
            let result: Vec<bool> = a.iter().zip(b.iter()).map(|(x, y)| *x != 0.0 || *y != 0.0).collect();
            if result.len() == 1 { Ok(Value::Bool(result[0])) } else { Ok(Value::BoolVec(result)) }
        }
        BinOp::Match => {
            Ok(Value::Bool(left == right))
        }
        BinOp::Join => eval_join(left, right),
        BinOp::Take => eval_take(left, right),
        BinOp::Drop => eval_drop(left, right),
        BinOp::Find => eval_find(left, right),
        BinOp::In => eval_in(left, right),
        BinOp::Like => {
            if let (Value::Symbol(s) | Value::Str(s), Value::Str(pat)) = (left, right) {
                let matched = simple_glob_match(pat, s);
                Ok(Value::Bool(matched))
            } else {
                bail!("like requires symbol/string and string pattern")
            }
        }
    }
}

fn eval_arith(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    // Duration arithmetic
    if let (Value::Duration(a), Value::Duration(b)) = (left, right) {
        return match op {
            BinOp::Add => Ok(Value::Duration(a + b)),
            BinOp::Sub => Ok(Value::Duration(a - b)),
            _ => bail!("unsupported duration op"),
        };
    }
    if let (Value::Timestamp(a), Value::Duration(b)) = (left, right) {
        return match op {
            BinOp::Add => Ok(Value::Timestamp(a + b)),
            BinOp::Sub => Ok(Value::Timestamp(a - b)),
            _ => bail!("unsupported timestamp op"),
        };
    }
    if let (Value::Timestamp(a), Value::Timestamp(b)) = (left, right) {
        if op == BinOp::Sub {
            return Ok(Value::Duration(a - b));
        }
    }

    // Fast-path: IntVec × IntVec (avoid float promotion)
    if let (Value::IntVec(a), Value::IntVec(b)) = (left, right) {
        if a.len() == b.len() {
            let result: std::result::Result<Vec<i64>, ()> =
                a.iter().zip(b.iter()).map(|(x, y)| match op {
                    BinOp::Add => Ok(x + y),
                    BinOp::Sub => Ok(x - y),
                    BinOp::Mul => Ok(x * y),
                    BinOp::Mod => if *y != 0 { Ok(x % y) } else { Ok(0) },
                    BinOp::Div | BinOp::Pow => Err(()),
                    _ => unreachable!(),
                }).collect();
            if let Ok(result) = result {
                return Ok(Value::IntVec(result));
            }
        }
    }

    // Fast-path: IntVec × Int scalar broadcast
    if let (Value::IntVec(a), Value::Int(b)) = (left, right) {
        if !matches!(op, BinOp::Div | BinOp::Pow) {
            let result: Vec<i64> = a.iter().map(|x| match op {
                BinOp::Add => x + b,
                BinOp::Sub => x - b,
                BinOp::Mul => x * b,
                BinOp::Mod => if *b != 0 { x % b } else { 0 },
                _ => unreachable!(),
            }).collect();
            return Ok(Value::IntVec(result));
        }
    }

    // Fast-path: Int scalar × IntVec broadcast
    if let (Value::Int(a), Value::IntVec(b)) = (left, right) {
        if !matches!(op, BinOp::Div | BinOp::Pow) {
            let result: Vec<i64> = b.iter().map(|y| match op {
                BinOp::Add => a + y,
                BinOp::Sub => a - y,
                BinOp::Mul => a * y,
                BinOp::Mod => if *y != 0 { a % y } else { 0 },
                _ => unreachable!(),
            }).collect();
            return Ok(Value::IntVec(result));
        }
    }

    // Fast-path: FloatVec × FloatVec (avoid broadcast check overhead)
    if let (Value::FloatVec(a), Value::FloatVec(b)) = (left, right) {
        if a.len() == b.len() {
            let result: Vec<f64> = a.iter().zip(b.iter()).map(|(x, y)| match op {
                BinOp::Add => x + y,
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div => x / y,
                BinOp::Pow => x.powf(*y),
                BinOp::Mod => x % y,
                _ => unreachable!(),
            }).collect();
            return Ok(Value::FloatVec(result));
        }
    }

    let (a, b) = Value::broadcast_float_pair(left, right)?;
    let result: Vec<f64> = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| match op {
            BinOp::Add => x + y,
            BinOp::Sub => x - y,
            BinOp::Mul => x * y,
            BinOp::Div => x / y,
            BinOp::Pow => x.powf(*y),
            BinOp::Mod => x % y,
            _ => unreachable!(),
        })
        .collect();

    if result.len() == 1 {
        // Check if we should return int
        if matches!((left, right), (Value::Int(_), Value::Int(_)))
            && !matches!(op, BinOp::Div | BinOp::Pow)
        {
            Ok(Value::Int(result[0] as i64))
        } else {
            Ok(Value::Float(result[0]))
        }
    } else if matches!((left, right), (Value::IntVec(_), Value::IntVec(_)) | (Value::IntVec(_), Value::Int(_)) | (Value::Int(_), Value::IntVec(_)))
        && !matches!(op, BinOp::Div | BinOp::Pow)
    {
        Ok(Value::IntVec(result.iter().map(|x| *x as i64).collect()))
    } else {
        Ok(Value::FloatVec(result))
    }
}

fn eval_comparison(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    // Fast-path: IntVec × Int scalar (very common: `x > 5`)
    if let (Value::IntVec(v), Value::Int(s)) = (left, right) {
        let result: Vec<bool> = v.iter().map(|x| match op {
            BinOp::Eq => *x == *s,
            BinOp::Neq => *x != *s,
            BinOp::Lt => *x < *s,
            BinOp::Gt => *x > *s,
            BinOp::Lte => *x <= *s,
            BinOp::Gte => *x >= *s,
            _ => unreachable!(),
        }).collect();
        return Ok(Value::BoolVec(result));
    }

    // Fast-path: FloatVec × Float scalar
    if let (Value::FloatVec(v), Value::Float(s)) = (left, right) {
        let result: Vec<bool> = v.iter().map(|x| match op {
            BinOp::Eq => (*x - *s).abs() < f64::EPSILON,
            BinOp::Neq => (*x - *s).abs() >= f64::EPSILON,
            BinOp::Lt => *x < *s,
            BinOp::Gt => *x > *s,
            BinOp::Lte => *x <= *s,
            BinOp::Gte => *x >= *s,
            _ => unreachable!(),
        }).collect();
        return Ok(Value::BoolVec(result));
    }

    let (a, b) = Value::broadcast_float_pair(left, right)?;
    let result: Vec<bool> = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| match op {
            BinOp::Eq => (x - y).abs() < f64::EPSILON || (x.is_nan() && y.is_nan()),
            BinOp::Neq => (x - y).abs() >= f64::EPSILON && !(x.is_nan() && y.is_nan()),
            BinOp::Lt => x < y,
            BinOp::Gt => x > y,
            BinOp::Lte => x <= y,
            BinOp::Gte => x >= y,
            _ => unreachable!(),
        })
        .collect();

    if result.len() == 1 {
        Ok(Value::Bool(result[0]))
    } else {
        Ok(Value::BoolVec(result))
    }
}

fn eval_join(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::IntVec(a), Value::IntVec(b)) => {
            Ok(Value::IntVec([a.as_slice(), b.as_slice()].concat()))
        }
        (Value::FloatVec(a), Value::FloatVec(b)) => {
            Ok(Value::FloatVec([a.as_slice(), b.as_slice()].concat()))
        }
        (Value::BoolVec(a), Value::BoolVec(b)) => {
            Ok(Value::BoolVec([a.as_slice(), b.as_slice()].concat()))
        }
        (Value::SymVec(a), Value::SymVec(b)) => {
            Ok(Value::SymVec([a.as_slice(), b.as_slice()].concat()))
        }
        (Value::IntVec(a), Value::Int(b)) => {
            let mut v = a.clone();
            v.push(*b);
            Ok(Value::IntVec(v))
        }
        (Value::Int(a), Value::IntVec(b)) => {
            let mut v = vec![*a];
            v.extend(b);
            Ok(Value::IntVec(v))
        }
        (Value::FloatVec(a), Value::Float(b)) => {
            let mut v = a.clone();
            v.push(*b);
            Ok(Value::FloatVec(v))
        }
        (Value::Str(a), Value::Str(b)) => {
            Ok(Value::Str(format!("{a}{b}")))
        }
        (Value::List(a), Value::List(b)) => {
            Ok(Value::List([a.as_slice(), b.as_slice()].concat()))
        }
        _ => {
            // General case: wrap in list
            Ok(Value::List(vec![left.clone(), right.clone()]))
        }
    }
}

fn eval_take(left: &Value, right: &Value) -> Result<Value> {
    let n = left.as_int()? as usize;
    match right {
        Value::IntVec(v) => Ok(Value::IntVec(v.iter().take(n).copied().collect())),
        Value::FloatVec(v) => Ok(Value::FloatVec(v.iter().take(n).copied().collect())),
        Value::BoolVec(v) => Ok(Value::BoolVec(v.iter().take(n).copied().collect())),
        Value::SymVec(v) => Ok(Value::SymVec(v.iter().take(n).cloned().collect())),
        Value::List(v) => Ok(Value::List(v.iter().take(n).cloned().collect())),
        Value::Str(s) => Ok(Value::Str(s.chars().take(n).collect())),
        _ => bail!("cannot take from {}", right.type_name()),
    }
}

fn eval_drop(left: &Value, right: &Value) -> Result<Value> {
    let n = left.as_int()? as usize;
    match right {
        Value::IntVec(v) => Ok(Value::IntVec(v.iter().skip(n).copied().collect())),
        Value::FloatVec(v) => Ok(Value::FloatVec(v.iter().skip(n).copied().collect())),
        Value::BoolVec(v) => Ok(Value::BoolVec(v.iter().skip(n).copied().collect())),
        Value::SymVec(v) => Ok(Value::SymVec(v.iter().skip(n).cloned().collect())),
        Value::List(v) => Ok(Value::List(v.iter().skip(n).cloned().collect())),
        Value::Str(s) => Ok(Value::Str(s.chars().skip(n).collect())),
        _ => bail!("cannot drop from {}", right.type_name()),
    }
}

fn eval_find(haystack: &Value, needle: &Value) -> Result<Value> {
    match haystack {
        Value::IntVec(v) => {
            let n = needle.as_int()?;
            let idx = v.iter().position(|x| *x == n).map(|i| i as i64).unwrap_or(-1);
            Ok(Value::Int(idx))
        }
        Value::FloatVec(v) => {
            let n = needle.as_float()?;
            let idx = v.iter().position(|x| (*x - n).abs() < f64::EPSILON).map(|i| i as i64).unwrap_or(-1);
            Ok(Value::Int(idx))
        }
        _ => bail!("cannot find in {}", haystack.type_name()),
    }
}

fn eval_in(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::IntVec(items), Value::IntVec(set)) => {
            Ok(Value::BoolVec(items.iter().map(|x| set.contains(x)).collect()))
        }
        (Value::Int(item), Value::IntVec(set)) => {
            Ok(Value::Bool(set.contains(item)))
        }
        (Value::Symbol(item), Value::SymVec(set)) => {
            Ok(Value::Bool(set.contains(item)))
        }
        _ => bail!("unsupported types for 'in'"),
    }
}

fn eval_unary(op: UnaryOp, val: &Value) -> Result<Value> {
    match op {
        UnaryOp::Neg => {
            match val {
                Value::Int(v) => Ok(Value::Int(-v)),
                Value::Float(v) => Ok(Value::Float(-v)),
                Value::IntVec(v) => Ok(Value::IntVec(v.iter().map(|x| -x).collect())),
                Value::FloatVec(v) => Ok(Value::FloatVec(v.iter().map(|x| -x).collect())),
                _ => bail!("cannot negate {}", val.type_name()),
            }
        }
        UnaryOp::Not => {
            match val {
                Value::Bool(b) => Ok(Value::Bool(!b)),
                Value::BoolVec(v) => Ok(Value::BoolVec(v.iter().map(|b| !b).collect())),
                _ => bail!("cannot apply 'not' to {}", val.type_name()),
            }
        }
        UnaryOp::Abs => float_map(val, |x| x.abs()),
        UnaryOp::Sqrt => float_map(val, |x| x.sqrt()),
        UnaryOp::Exp => float_map(val, |x| x.exp()),
        UnaryOp::Log => float_map(val, |x| x.ln()),
        UnaryOp::Ceil => float_map(val, |x| x.ceil()),
        UnaryOp::Floor => float_map(val, |x| x.floor()),
        UnaryOp::Signum => float_map(val, |x| x.signum()),
        UnaryOp::Reciprocal => float_map(val, |x| 1.0 / x),

        // Aggregations
        UnaryOp::Sum => {
            let v = val.to_float_vec()?;
            Ok(Value::Float(v.iter().sum()))
        }
        UnaryOp::Avg | UnaryOp::Mean => {
            let v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::Null); }
            Ok(Value::Float(v.iter().sum::<f64>() / v.len() as f64))
        }
        UnaryOp::Min => {
            let v = val.to_float_vec()?;
            v.iter().copied().reduce(f64::min).map(Value::Float).ok_or_else(|| anyhow::anyhow!("min of empty"))
        }
        UnaryOp::Max => {
            let v = val.to_float_vec()?;
            v.iter().copied().reduce(f64::max).map(Value::Float).ok_or_else(|| anyhow::anyhow!("max of empty"))
        }
        UnaryOp::Count => {
            Ok(Value::Int(val.count() as i64))
        }
        UnaryOp::First => {
            match val {
                Value::IntVec(v) => v.first().map(|x| Value::Int(*x)).ok_or_else(|| anyhow::anyhow!("first of empty")),
                Value::FloatVec(v) => v.first().map(|x| Value::Float(*x)).ok_or_else(|| anyhow::anyhow!("first of empty")),
                Value::List(v) => v.first().cloned().ok_or_else(|| anyhow::anyhow!("first of empty")),
                _ => Ok(val.clone()),
            }
        }
        UnaryOp::Last => {
            match val {
                Value::IntVec(v) => v.last().map(|x| Value::Int(*x)).ok_or_else(|| anyhow::anyhow!("last of empty")),
                Value::FloatVec(v) => v.last().map(|x| Value::Float(*x)).ok_or_else(|| anyhow::anyhow!("last of empty")),
                Value::List(v) => v.last().cloned().ok_or_else(|| anyhow::anyhow!("last of empty")),
                _ => Ok(val.clone()),
            }
        }
        UnaryOp::Med => {
            let mut v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::Null); }
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = v.len() / 2;
            if v.len() % 2 == 0 {
                Ok(Value::Float((v[mid - 1] + v[mid]) / 2.0))
            } else {
                Ok(Value::Float(v[mid]))
            }
        }
        UnaryOp::Dev => {
            let v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::Null); }
            let mean = v.iter().sum::<f64>() / v.len() as f64;
            let variance = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64;
            Ok(Value::Float(variance.sqrt()))
        }
        UnaryOp::Var => {
            let v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::Null); }
            let mean = v.iter().sum::<f64>() / v.len() as f64;
            let variance = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64;
            Ok(Value::Float(variance))
        }

        // Scans
        UnaryOp::Sums => scan_op(val, 0.0, |a, b| a + b),
        UnaryOp::Avgs => {
            let v = val.to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            let mut sum = 0.0;
            for (i, x) in v.iter().enumerate() {
                sum += x;
                result.push(sum / (i + 1) as f64);
            }
            Ok(Value::FloatVec(result))
        }
        UnaryOp::Mins => scan_op(val, f64::INFINITY, f64::min),
        UnaryOp::Maxs => scan_op(val, f64::NEG_INFINITY, f64::max),
        UnaryOp::Prds => scan_op(val, 1.0, |a, b| a * b),

        // Structural
        UnaryOp::Til => {
            let n = val.as_int()?;
            Ok(Value::IntVec((0..n).collect()))
        }
        UnaryOp::Rev => {
            match val {
                Value::IntVec(v) => { let mut r = v.clone(); r.reverse(); Ok(Value::IntVec(r)) }
                Value::FloatVec(v) => { let mut r = v.clone(); r.reverse(); Ok(Value::FloatVec(r)) }
                Value::BoolVec(v) => { let mut r = v.clone(); r.reverse(); Ok(Value::BoolVec(r)) }
                Value::List(v) => { let mut r = v.clone(); r.reverse(); Ok(Value::List(r)) }
                _ => bail!("cannot reverse {}", val.type_name()),
            }
        }
        UnaryOp::Asc => {
            match val {
                Value::IntVec(v) => { let mut s = v.clone(); s.sort(); Ok(Value::IntVec(s)) }
                Value::FloatVec(v) => {
                    let mut s = v.clone();
                    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    Ok(Value::FloatVec(s))
                }
                _ => bail!("cannot sort {}", val.type_name()),
            }
        }
        UnaryOp::Desc => {
            match val {
                Value::IntVec(v) => { let mut s = v.clone(); s.sort(); s.reverse(); Ok(Value::IntVec(s)) }
                Value::FloatVec(v) => {
                    let mut s = v.clone();
                    s.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
                    Ok(Value::FloatVec(s))
                }
                _ => bail!("cannot sort {}", val.type_name()),
            }
        }
        UnaryOp::Distinct => {
            match val {
                Value::IntVec(v) => {
                    let mut seen = Vec::new();
                    for x in v { if !seen.contains(x) { seen.push(*x); } }
                    Ok(Value::IntVec(seen))
                }
                Value::FloatVec(v) => {
                    let mut seen = Vec::new();
                    for x in v {
                        if !seen.iter().any(|s: &f64| (s - x).abs() < f64::EPSILON) {
                            seen.push(*x);
                        }
                    }
                    Ok(Value::FloatVec(seen))
                }
                Value::SymVec(v) => {
                    let mut seen = Vec::new();
                    for x in v { if !seen.contains(x) { seen.push(x.clone()); } }
                    Ok(Value::SymVec(seen))
                }
                _ => bail!("cannot distinct {}", val.type_name()),
            }
        }
        UnaryOp::Group => {
            match val {
                Value::IntVec(v) => {
                    let mut groups: BTreeMap<String, Value> = BTreeMap::new();
                    for (i, x) in v.iter().enumerate() {
                        let key = x.to_string();
                        match groups.get_mut(&key) {
                            Some(Value::IntVec(indices)) => indices.push(i as i64),
                            _ => { groups.insert(key, Value::IntVec(vec![i as i64])); }
                        }
                    }
                    Ok(Value::Dict(groups))
                }
                Value::SymVec(v) => {
                    let mut groups: BTreeMap<String, Value> = BTreeMap::new();
                    for (i, x) in v.iter().enumerate() {
                        match groups.get_mut(x) {
                            Some(Value::IntVec(indices)) => indices.push(i as i64),
                            _ => { groups.insert(x.clone(), Value::IntVec(vec![i as i64])); }
                        }
                    }
                    Ok(Value::Dict(groups))
                }
                _ => bail!("cannot group {}", val.type_name()),
            }
        }
        UnaryOp::Flip => {
            // Transpose list of lists
            if let Value::List(rows) = val {
                if rows.is_empty() { return Ok(val.clone()); }
                let ncols = rows[0].count();
                let mut cols: Vec<Vec<Value>> = (0..ncols).map(|_| Vec::new()).collect();
                for row in rows {
                    if let Value::List(items) = row {
                        for (j, item) in items.iter().enumerate() {
                            if j < ncols { cols[j].push(item.clone()); }
                        }
                    }
                }
                Ok(Value::List(cols.into_iter().map(Value::List).collect()))
            } else {
                bail!("cannot flip {}", val.type_name())
            }
        }
        UnaryOp::Raze => {
            if let Value::List(items) = val {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Value::List(inner) => result.extend(inner.iter().cloned()),
                        other => result.push(other.clone()),
                    }
                }
                Ok(Value::List(result))
            } else {
                Ok(val.clone())
            }
        }
        UnaryOp::Where => {
            // Bool vector → indices where true
            if let Value::BoolVec(v) = val {
                let indices: Vec<i64> = v.iter().enumerate()
                    .filter(|(_, b)| **b)
                    .map(|(i, _)| i as i64)
                    .collect();
                Ok(Value::IntVec(indices))
            } else {
                bail!("where expects bool vector, got {}", val.type_name())
            }
        }

        // Time-series
        UnaryOp::Deltas => {
            let v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::FloatVec(vec![])); }
            let mut result = vec![v[0]];
            for i in 1..v.len() {
                result.push(v[i] - v[i - 1]);
            }
            Ok(Value::FloatVec(result))
        }
        UnaryOp::Ratios => {
            let v = val.to_float_vec()?;
            if v.is_empty() { return Ok(Value::FloatVec(vec![])); }
            let mut result = vec![f64::NAN];
            for i in 1..v.len() {
                result.push(v[i] / v[i - 1]);
            }
            Ok(Value::FloatVec(result))
        }
        UnaryOp::Prev => {
            match val {
                Value::IntVec(v) if v.is_empty() => Ok(Value::IntVec(vec![])),
                Value::IntVec(v) => {
                    let mut r = vec![0i64];
                    r.extend_from_slice(&v[..v.len() - 1]);
                    Ok(Value::IntVec(r))
                }
                Value::FloatVec(v) if v.is_empty() => Ok(Value::FloatVec(vec![])),
                Value::FloatVec(v) => {
                    let mut r = vec![f64::NAN];
                    r.extend_from_slice(&v[..v.len() - 1]);
                    Ok(Value::FloatVec(r))
                }
                _ => bail!("cannot apply prev to {}", val.type_name()),
            }
        }
        UnaryOp::Next => {
            match val {
                Value::IntVec(v) if v.is_empty() => Ok(Value::IntVec(vec![])),
                Value::IntVec(v) => {
                    let mut r: Vec<i64> = v[1..].to_vec();
                    r.push(0);
                    Ok(Value::IntVec(r))
                }
                Value::FloatVec(v) if v.is_empty() => Ok(Value::FloatVec(vec![])),
                Value::FloatVec(v) => {
                    let mut r: Vec<f64> = v[1..].to_vec();
                    r.push(f64::NAN);
                    Ok(Value::FloatVec(r))
                }
                _ => bail!("cannot apply next to {}", val.type_name()),
            }
        }
        UnaryOp::Fills | UnaryOp::Ffill => {
            if let Value::FloatVec(v) = val {
                let mut result = v.clone();
                for i in 1..result.len() {
                    if result[i].is_nan() {
                        result[i] = result[i - 1];
                    }
                }
                Ok(Value::FloatVec(result))
            } else {
                Ok(val.clone())
            }
        }
        UnaryOp::Bfill => {
            if let Value::FloatVec(v) = val {
                let mut result = v.clone();
                for i in (0..result.len().saturating_sub(1)).rev() {
                    if result[i].is_nan() {
                        result[i] = result[i + 1];
                    }
                }
                Ok(Value::FloatVec(result))
            } else {
                Ok(val.clone())
            }
        }

        // Type
        UnaryOp::Type => Ok(Value::Symbol(val.type_name().to_string())),
        UnaryOp::IsNull => Ok(Value::Bool(matches!(val, Value::Null))),
        UnaryOp::Key => {
            if let Value::Dict(d) = val {
                Ok(Value::SymVec(d.keys().cloned().collect()))
            } else if let Value::Table(t) = val {
                Ok(Value::SymVec(t.columns.clone()))
            } else {
                bail!("key requires dict or table")
            }
        }
        UnaryOp::Value => {
            if let Value::Dict(d) = val {
                Ok(Value::List(d.values().cloned().collect()))
            } else {
                bail!("value requires dict")
            }
        }
        UnaryOp::ToString => Ok(Value::Str(format!("{val}"))),
        UnaryOp::Upper => {
            if let Value::Str(s) = val {
                Ok(Value::Str(s.to_uppercase()))
            } else {
                bail!("upper requires string")
            }
        }
        UnaryOp::Lower => {
            if let Value::Str(s) = val {
                Ok(Value::Str(s.to_lowercase()))
            } else {
                bail!("lower requires string")
            }
        }
        UnaryOp::Trim => {
            if let Value::Str(s) = val {
                Ok(Value::Str(s.trim().to_string()))
            } else {
                bail!("trim requires string")
            }
        }
        UnaryOp::Now => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64;
            Ok(Value::Timestamp(now))
        }
    }
}

fn float_map(val: &Value, f: impl Fn(f64) -> f64) -> Result<Value> {
    match val {
        Value::Float(v) => Ok(Value::Float(f(*v))),
        Value::Int(v) => Ok(Value::Float(f(*v as f64))),
        Value::FloatVec(v) => Ok(Value::FloatVec(v.iter().map(|x| f(*x)).collect())),
        Value::IntVec(v) => Ok(Value::FloatVec(v.iter().map(|x| f(*x as f64)).collect())),
        _ => bail!("cannot apply math function to {}", val.type_name()),
    }
}

fn scan_op(val: &Value, init: f64, f: impl Fn(f64, f64) -> f64) -> Result<Value> {
    let v = val.to_float_vec()?;
    if v.is_empty() {
        return Ok(Value::FloatVec(vec![]));
    }
    let mut result = Vec::with_capacity(v.len());
    let mut acc = f(init, v[0]);
    result.push(acc);
    for x in &v[1..] {
        acc = f(acc, *x);
        result.push(acc);
    }
    Ok(Value::FloatVec(result))
}

fn eval_apply(func: &Value, args: &[Value], env: &mut Env) -> Result<Value> {
    match func {
        Value::Lambda { params, body } => {
            env.push_scope();
            if params.is_empty() {
                // Implicit params: x, y, z
                if let Some(arg) = args.first() {
                    env.set("x".into(), arg.clone());
                }
                if let Some(arg) = args.get(1) {
                    env.set("y".into(), arg.clone());
                }
                if let Some(arg) = args.get(2) {
                    env.set("z".into(), arg.clone());
                }
            } else {
                for (i, param) in params.iter().enumerate() {
                    let val = args.get(i).cloned().unwrap_or(Value::Null);
                    env.set(param.clone(), val);
                }
            }
            let result = eval(body, env);
            env.pop_scope();
            result
        }
        Value::BuiltinFn(name) => {
            // Try monadic builtin first (single arg)
            if args.len() == 1 && is_monadic_builtin_name(name) {
                return eval_unary(monadic_from_builtin_name(name), &args[0]);
            }
            eval_builtin_fn(name, args)
        }
        // If it's a unary op applied via pipe
        _ => bail!("cannot apply {} as function", func.type_name()),
    }
}

fn eval_builtin_fn(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "mavg" => {
            if args.len() != 2 { bail!("mavg requires 2 args: window, vec"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                let window = &v[start..=i];
                result.push(window.iter().sum::<f64>() / window.len() as f64);
            }
            Ok(Value::FloatVec(result))
        }
        "msum" => {
            if args.len() != 2 { bail!("msum requires 2 args"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                result.push(v[start..=i].iter().sum());
            }
            Ok(Value::FloatVec(result))
        }
        "mmin" => {
            if args.len() != 2 { bail!("mmin requires 2 args"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                result.push(v[start..=i].iter().copied().reduce(f64::min).unwrap());
            }
            Ok(Value::FloatVec(result))
        }
        "mmax" => {
            if args.len() != 2 { bail!("mmax requires 2 args"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                result.push(v[start..=i].iter().copied().reduce(f64::max).unwrap());
            }
            Ok(Value::FloatVec(result))
        }
        "ema" => {
            if args.len() != 2 { bail!("ema requires 2 args: alpha, vec"); }
            let alpha = args[0].as_float()?;
            let v = args[1].to_float_vec()?;
            if v.is_empty() { return Ok(Value::FloatVec(vec![])); }
            let mut result = vec![v[0]];
            for i in 1..v.len() {
                result.push(alpha * v[i] + (1.0 - alpha) * result[i - 1]);
            }
            Ok(Value::FloatVec(result))
        }
        "xbar" => {
            if args.len() != 2 { bail!("xbar requires 2 args: duration, timestamps"); }
            let interval = match &args[0] {
                Value::Duration(ns) => *ns,
                Value::Int(n) => *n,
                _ => bail!("xbar first arg must be duration or int"),
            };
            match &args[1] {
                Value::TimestampVec(ts) => {
                    let bucketed: Vec<i64> = ts.iter().map(|t| (t / interval) * interval).collect();
                    Ok(Value::TimestampVec(bucketed))
                }
                Value::IntVec(v) => {
                    let bucketed: Vec<i64> = v.iter().map(|t| (t / interval) * interval).collect();
                    Ok(Value::IntVec(bucketed))
                }
                _ => bail!("xbar second arg must be timestamp or int vector"),
            }
        }
        "pct" => {
            if args.len() != 2 { bail!("pct requires 2 args: percentile, vec"); }
            let p = args[0].as_float()?;
            let mut v = args[1].to_float_vec()?;
            if v.is_empty() { return Ok(Value::Null); }
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = (p / 100.0 * (v.len() - 1) as f64).round() as usize;
            let idx = idx.min(v.len() - 1);
            Ok(Value::Float(v[idx]))
        }
        "wavg" => {
            if args.len() != 2 { bail!("wavg requires 2 args: weights, values"); }
            let w = args[0].to_float_vec()?;
            let v = args[1].to_float_vec()?;
            if w.len() != v.len() { bail!("wavg: length mismatch"); }
            let sum_wv: f64 = w.iter().zip(v.iter()).map(|(a, b)| a * b).sum();
            let sum_w: f64 = w.iter().sum();
            Ok(Value::Float(sum_wv / sum_w))
        }
        "cor" => {
            if args.len() != 2 { bail!("cor requires 2 args"); }
            let x = args[0].to_float_vec()?;
            let y = args[1].to_float_vec()?;
            if x.len() != y.len() { bail!("cor: length mismatch"); }
            let n = x.len() as f64;
            let mx = x.iter().sum::<f64>() / n;
            let my = y.iter().sum::<f64>() / n;
            let mut cov = 0.0;
            let mut sx = 0.0;
            let mut sy = 0.0;
            for i in 0..x.len() {
                let dx = x[i] - mx;
                let dy = y[i] - my;
                cov += dx * dy;
                sx += dx * dx;
                sy += dy * dy;
            }
            Ok(Value::Float(cov / (sx.sqrt() * sy.sqrt())))
        }
        "mdev" => {
            if args.len() != 2 { bail!("mdev requires 2 args: window, vec"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                let window = &v[start..=i];
                let mean = window.iter().sum::<f64>() / window.len() as f64;
                let variance = window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / window.len() as f64;
                result.push(variance.sqrt());
            }
            Ok(Value::FloatVec(result))
        }
        "mcount" => {
            if args.len() != 2 { bail!("mcount requires 2 args: window, vec"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                let count = v[start..=i].iter().filter(|x| !x.is_nan()).count();
                result.push(count as f64);
            }
            Ok(Value::FloatVec(result))
        }
        "wma" => {
            if args.len() != 2 { bail!("wma requires 2 args: window, vec"); }
            let n = args[0].as_int()? as usize;
            let v = args[1].to_float_vec()?;
            let mut result = Vec::with_capacity(v.len());
            for i in 0..v.len() {
                let start = i.saturating_sub(n - 1);
                let window = &v[start..=i];
                let mut weighted_sum = 0.0;
                let mut weight_total = 0.0;
                for (j, val) in window.iter().enumerate() {
                    let w = (j + 1) as f64;
                    weighted_sum += w * val;
                    weight_total += w;
                }
                result.push(weighted_sum / weight_total);
            }
            Ok(Value::FloatVec(result))
        }
        "resample" => {
            if args.len() != 4 { bail!("resample requires 4 args: duration, timestamps, values, agg_fn"); }
            let interval = match &args[0] {
                Value::Duration(ns) => *ns,
                Value::Int(n) => *n,
                _ => bail!("resample first arg must be duration or int"),
            };
            let timestamps = match &args[1] {
                Value::TimestampVec(ts) => ts.clone(),
                Value::IntVec(ts) => ts.clone(),
                _ => bail!("resample second arg must be timestamp or int vector"),
            };
            let values = args[2].to_float_vec()?;
            let agg_name = match &args[3] {
                Value::BuiltinFn(name) => name.clone(),
                _ => bail!("resample fourth arg must be a builtin aggregation function"),
            };
            if timestamps.len() != values.len() {
                bail!("resample: timestamps and values length mismatch");
            }
            let mut buckets: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
            for (ts, val) in timestamps.iter().zip(values.iter()) {
                let bucket = (ts / interval) * interval;
                buckets.entry(bucket).or_default().push(*val);
            }
            let mut ts_col = Vec::with_capacity(buckets.len());
            let mut val_col = Vec::with_capacity(buckets.len());
            for (bucket, vals) in &buckets {
                ts_col.push(*bucket);
                let bucket_val = Value::FloatVec(vals.clone());
                let agg_val = if is_monadic_builtin_name(&agg_name) {
                    eval_unary(monadic_from_builtin_name(&agg_name), &bucket_val)?
                } else {
                    eval_builtin_fn(&agg_name, &[bucket_val])?
                };
                match agg_val {
                    Value::Float(f) => val_col.push(f),
                    Value::FloatVec(v) => val_col.push(*v.first().unwrap_or(&f64::NAN)),
                    _ => val_col.push(f64::NAN),
                }
            }
            let is_timestamp = matches!(&args[1], Value::TimestampVec(_));
            let mut data = BTreeMap::new();
            let columns = vec!["ts".to_string(), "value".to_string()];
            if is_timestamp {
                data.insert("ts".to_string(), Value::TimestampVec(ts_col));
            } else {
                data.insert("ts".to_string(), Value::IntVec(ts_col));
            }
            data.insert("value".to_string(), Value::FloatVec(val_col));
            Ok(Value::Table(Table { columns, data }))
        }
        "asof" => {
            if args.len() != 4 { bail!("asof requires 4 args: ts_left, val_left, ts_right, val_right"); }
            let ts_left = match &args[0] {
                Value::TimestampVec(ts) => ts.clone(),
                Value::IntVec(ts) => ts.clone(),
                _ => bail!("asof first arg must be timestamp or int vector"),
            };
            let val_left = args[1].to_float_vec()?;
            let ts_right = match &args[2] {
                Value::TimestampVec(ts) => ts.clone(),
                Value::IntVec(ts) => ts.clone(),
                _ => bail!("asof third arg must be timestamp or int vector"),
            };
            let val_right = args[3].to_float_vec()?;
            if ts_left.len() != val_left.len() {
                bail!("asof: ts_left and val_left length mismatch");
            }
            if ts_right.len() != val_right.len() {
                bail!("asof: ts_right and val_right length mismatch");
            }
            let mut right_col = Vec::with_capacity(ts_left.len());
            for &tl in &ts_left {
                let mut best: Option<f64> = None;
                for (j, &tr) in ts_right.iter().enumerate() {
                    if tr <= tl {
                        best = Some(val_right[j]);
                    } else {
                        break;
                    }
                }
                right_col.push(best.unwrap_or(f64::NAN));
            }
            let is_timestamp = matches!(&args[0], Value::TimestampVec(_));
            let mut data = BTreeMap::new();
            let columns = vec!["ts".to_string(), "left".to_string(), "right".to_string()];
            if is_timestamp {
                data.insert("ts".to_string(), Value::TimestampVec(ts_left));
            } else {
                data.insert("ts".to_string(), Value::IntVec(ts_left));
            }
            data.insert("left".to_string(), Value::FloatVec(val_left));
            data.insert("right".to_string(), Value::FloatVec(right_col));
            Ok(Value::Table(Table { columns, data }))
        }
        _ => bail!("unknown builtin function: {name}"),
    }
}

fn eval_member(obj: &Value, field: &str) -> Result<Value> {
    match obj {
        Value::Dict(d) => {
            d.get(field).cloned().ok_or_else(|| anyhow::anyhow!("key not found: {field}"))
        }
        Value::Table(t) => {
            t.data.get(field).cloned().ok_or_else(|| anyhow::anyhow!("column not found: {field}"))
        }
        Value::Timestamp(ns) => {
            let dt = chrono::DateTime::from_timestamp_nanos(*ns);
            match field {
                "year" => Ok(Value::Int(chrono::Datelike::year(&dt) as i64)),
                "month" => Ok(Value::Int(chrono::Datelike::month(&dt) as i64)),
                "day" => Ok(Value::Int(chrono::Datelike::day(&dt) as i64)),
                "hour" => Ok(Value::Int(chrono::Timelike::hour(&dt) as i64)),
                "minute" => Ok(Value::Int(chrono::Timelike::minute(&dt) as i64)),
                "second" => Ok(Value::Int(chrono::Timelike::second(&dt) as i64)),
                "week" => Ok(Value::Int(chrono::Datelike::iso_week(&dt).week() as i64)),
                "dow" => Ok(Value::Int(chrono::Datelike::weekday(&dt).num_days_from_monday() as i64)),
                _ => bail!("unknown timestamp field: {field}"),
            }
        }
        Value::TimestampVec(ts_vec) => {
            let extract: fn(&chrono::DateTime<chrono::Utc>) -> i64 = match field {
                "year" => |dt| chrono::Datelike::year(dt) as i64,
                "month" => |dt| chrono::Datelike::month(dt) as i64,
                "day" => |dt| chrono::Datelike::day(dt) as i64,
                "hour" => |dt| chrono::Timelike::hour(dt) as i64,
                "minute" => |dt| chrono::Timelike::minute(dt) as i64,
                "second" => |dt| chrono::Timelike::second(dt) as i64,
                "week" => |dt| chrono::Datelike::iso_week(dt).week() as i64,
                "dow" => |dt| chrono::Datelike::weekday(dt).num_days_from_monday() as i64,
                _ => bail!("unknown timestamp field: {field}"),
            };
            let vals = ts_vec.iter().map(|ns| {
                let dt = chrono::DateTime::from_timestamp_nanos(*ns);
                extract(&dt)
            }).collect();
            Ok(Value::IntVec(vals))
        }
        _ => bail!("cannot access member '{field}' on {}", obj.type_name()),
    }
}

fn eval_index(obj: &Value, idx: &Value) -> Result<Value> {
    match (obj, idx) {
        (Value::IntVec(v), Value::Int(i)) => {
            let i = normalize_index(*i, v.len())?;
            Ok(Value::Int(v[i]))
        }
        (Value::FloatVec(v), Value::Int(i)) => {
            let i = normalize_index(*i, v.len())?;
            Ok(Value::Float(v[i]))
        }
        (Value::BoolVec(v), Value::Int(i)) => {
            let i = normalize_index(*i, v.len())?;
            Ok(Value::Bool(v[i]))
        }
        (Value::List(v), Value::Int(i)) => {
            let i = normalize_index(*i, v.len())?;
            Ok(v[i].clone())
        }
        (Value::IntVec(v), Value::IntVec(indices)) => {
            let result: Result<Vec<i64>> = indices
                .iter()
                .map(|i| {
                    let i = normalize_index(*i, v.len())?;
                    Ok(v[i])
                })
                .collect();
            Ok(Value::IntVec(result?))
        }
        (Value::FloatVec(v), Value::IntVec(indices)) => {
            let result: Result<Vec<f64>> = indices
                .iter()
                .map(|i| {
                    let i = normalize_index(*i, v.len())?;
                    Ok(v[i])
                })
                .collect();
            Ok(Value::FloatVec(result?))
        }
        // Boolean indexing: x[bool_vec] → filter
        (Value::IntVec(v), Value::BoolVec(mask)) => {
            if v.len() != mask.len() { bail!("boolean index length mismatch"); }
            Ok(Value::IntVec(v.iter().zip(mask).filter(|(_, b)| **b).map(|(x, _)| *x).collect()))
        }
        (Value::FloatVec(v), Value::BoolVec(mask)) => {
            if v.len() != mask.len() { bail!("boolean index length mismatch"); }
            Ok(Value::FloatVec(v.iter().zip(mask).filter(|(_, b)| **b).map(|(x, _)| *x).collect()))
        }
        (Value::Dict(d), Value::Symbol(key)) => {
            d.get(key).cloned().ok_or_else(|| anyhow::anyhow!("key not found: {key}"))
        }
        (Value::Table(t), Value::Symbol(col)) => {
            t.data.get(col).cloned().ok_or_else(|| anyhow::anyhow!("column not found: {col}"))
        }
        (Value::Table(t), Value::Int(row)) => {
            let i = normalize_index(*row, t.data.values().next().map_or(0, |v| v.count()))?;
            let mut row_dict = BTreeMap::new();
            for (col, vals) in &t.data {
                row_dict.insert(col.clone(), eval_index(vals, &Value::Int(i as i64))?);
            }
            Ok(Value::Dict(row_dict))
        }
        _ => bail!("cannot index {} with {}", obj.type_name(), idx.type_name()),
    }
}

fn eval_iterator(func: &Value, iter: IterKind, arg: &Value, env: &mut Env) -> Result<Value> {
    match iter {
        IterKind::Each => {
            match arg {
                Value::List(items) => {
                    let results: Result<Vec<Value>> = items
                        .iter()
                        .map(|item| eval_apply(func, &[item.clone()], env))
                        .collect();
                    Ok(Value::List(results?))
                }
                Value::IntVec(v) => {
                    let results: Result<Vec<Value>> = v
                        .iter()
                        .map(|item| eval_apply(func, &[Value::Int(*item)], env))
                        .collect();
                    Ok(Value::List(results?))
                }
                _ => eval_apply(func, &[arg.clone()], env),
            }
        }
        IterKind::Over => {
            // Reduce: f/ vec
            let v = match arg {
                Value::IntVec(v) if v.len() >= 2 => {
                    let vals: Vec<Value> = v.iter().map(|x| Value::Int(*x)).collect();
                    vals
                }
                Value::FloatVec(v) if v.len() >= 2 => {
                    let vals: Vec<Value> = v.iter().map(|x| Value::Float(*x)).collect();
                    vals
                }
                Value::List(v) if v.len() >= 2 => v.clone(),
                _ => return Ok(arg.clone()),
            };
            let mut acc = v[0].clone();
            for item in &v[1..] {
                acc = eval_apply(func, &[acc, item.clone()], env)?;
            }
            Ok(acc)
        }
        IterKind::Scan => {
            // Scan: f\ vec — like reduce but keeping intermediates
            let v = match arg {
                Value::IntVec(v) => v.iter().map(|x| Value::Int(*x)).collect::<Vec<_>>(),
                Value::FloatVec(v) => v.iter().map(|x| Value::Float(*x)).collect::<Vec<_>>(),
                Value::List(v) => v.clone(),
                _ => return Ok(arg.clone()),
            };
            if v.is_empty() { return Ok(Value::List(vec![])); }
            let mut results = vec![v[0].clone()];
            let mut acc = v[0].clone();
            for item in &v[1..] {
                acc = eval_apply(func, &[acc, item.clone()], env)?;
                results.push(acc.clone());
            }
            Ok(Value::List(results))
        }
        IterKind::EachPrior => {
            // f': vec — apply f to consecutive pairs
            let v = match arg {
                Value::IntVec(v) => v.iter().map(|x| Value::Int(*x)).collect::<Vec<_>>(),
                Value::FloatVec(v) => v.iter().map(|x| Value::Float(*x)).collect::<Vec<_>>(),
                Value::List(v) => v.clone(),
                _ => return Ok(arg.clone()),
            };
            if v.is_empty() { return Ok(Value::List(vec![])); }
            let mut results = vec![v[0].clone()];
            for i in 1..v.len() {
                results.push(eval_apply(func, &[v[i].clone(), v[i - 1].clone()], env)?);
            }
            Ok(Value::List(results))
        }
    }
}

fn normalize_index(i: i64, len: usize) -> Result<usize> {
    let idx = if i < 0 { len as i64 + i } else { i };
    if idx < 0 || idx as usize >= len {
        bail!("index {i} out of bounds for length {len}");
    }
    Ok(idx as usize)
}

fn duration_to_nanos(value: u64, unit: DurationUnit) -> i64 {
    let multiplier: i64 = match unit {
        DurationUnit::Nanoseconds => 1,
        DurationUnit::Microseconds => 1_000,
        DurationUnit::Milliseconds => 1_000_000,
        DurationUnit::Seconds => 1_000_000_000,
        DurationUnit::Minutes => 60_000_000_000,
        DurationUnit::Hours => 3_600_000_000_000,
        DurationUnit::Days => 86_400_000_000_000,
        DurationUnit::Weeks => 604_800_000_000_000,
    };
    value as i64 * multiplier
}

fn parse_timestamp(s: &str) -> Result<i64> {
    // Parse YYYY.MM.DDDhh:mm:ss
    let s = s.replace('.', "-");
    // Now: 2024-01-15D14:30:00
    let dt = if let Some((date, time)) = s.split_once('D') {
        let date = date.replace('-', "-");
        let datetime_str = format!("{date}T{time}");
        chrono::NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%dT%H:%M"))
            .map_err(|e| anyhow::anyhow!("invalid timestamp: {e}"))?
    } else {
        bail!("invalid timestamp format: expected YYYY.MM.DDDhh:mm:ss");
    };
    Ok(dt.and_utc().timestamp_nanos_opt().unwrap_or(0))
}

fn is_monadic_builtin_name(name: &str) -> bool {
    matches!(
        name,
        "neg" | "not" | "abs" | "sqrt" | "exp" | "log" | "ceil" | "floor"
            | "signum" | "reciprocal"
            | "sum" | "avg" | "mean" | "min" | "max" | "count" | "first" | "last"
            | "med" | "dev" | "var"
            | "sums" | "avgs" | "mins" | "maxs" | "prds"
            | "til" | "rev" | "asc" | "desc" | "distinct" | "group" | "flip" | "raze"
            | "where"
            | "deltas" | "ratios" | "prev" | "next" | "fills" | "ffill" | "bfill"
            | "type" | "null" | "key" | "value" | "string"
            | "upper" | "lower" | "trim"
    )
}

fn monadic_from_builtin_name(name: &str) -> UnaryOp {
    match name {
        "neg" => UnaryOp::Neg,
        "not" => UnaryOp::Not,
        "abs" => UnaryOp::Abs,
        "sqrt" => UnaryOp::Sqrt,
        "exp" => UnaryOp::Exp,
        "log" => UnaryOp::Log,
        "ceil" => UnaryOp::Ceil,
        "floor" => UnaryOp::Floor,
        "signum" => UnaryOp::Signum,
        "reciprocal" => UnaryOp::Reciprocal,
        "sum" => UnaryOp::Sum,
        "avg" | "mean" => UnaryOp::Avg,
        "min" => UnaryOp::Min,
        "max" => UnaryOp::Max,
        "count" => UnaryOp::Count,
        "first" => UnaryOp::First,
        "last" => UnaryOp::Last,
        "med" => UnaryOp::Med,
        "dev" => UnaryOp::Dev,
        "var" => UnaryOp::Var,
        "sums" => UnaryOp::Sums,
        "avgs" => UnaryOp::Avgs,
        "mins" => UnaryOp::Mins,
        "maxs" => UnaryOp::Maxs,
        "prds" => UnaryOp::Prds,
        "til" => UnaryOp::Til,
        "rev" => UnaryOp::Rev,
        "asc" => UnaryOp::Asc,
        "desc" => UnaryOp::Desc,
        "distinct" => UnaryOp::Distinct,
        "group" => UnaryOp::Group,
        "flip" => UnaryOp::Flip,
        "raze" => UnaryOp::Raze,
        "where" => UnaryOp::Where,
        "deltas" => UnaryOp::Deltas,
        "ratios" => UnaryOp::Ratios,
        "prev" => UnaryOp::Prev,
        "next" => UnaryOp::Next,
        "fills" => UnaryOp::Fills,
        "ffill" => UnaryOp::Ffill,
        "bfill" => UnaryOp::Bfill,
        "type" => UnaryOp::Type,
        "null" => UnaryOp::IsNull,
        "key" => UnaryOp::Key,
        "value" => UnaryOp::Value,
        "string" => UnaryOp::ToString,
        "upper" => UnaryOp::Upper,
        "lower" => UnaryOp::Lower,
        "trim" => UnaryOp::Trim,
        _ => unreachable!("not a monadic builtin: {name}"),
    }
}

// Public wrappers for use by the db integration module.

pub fn eval_unary_pub(op: UnaryOp, val: &Value) -> Result<Value> {
    eval_unary(op, val)
}

pub fn eval_binop_pub(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    eval_binop(op, left, right)
}

pub fn eval_apply_pub(func: &Value, args: &[Value], env: &mut Env) -> Result<Value> {
    eval_apply(func, args, env)
}

pub fn eval_index_pub(obj: &Value, idx: &Value) -> Result<Value> {
    eval_index(obj, idx)
}

pub fn eval_member_pub(obj: &Value, field: &str) -> Result<Value> {
    eval_member(obj, field)
}

fn simple_glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    glob_match_inner(&pattern, &text, 0, 0)
}

fn glob_match_inner(pattern: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }
    if pattern[pi] == '*' {
        // Try matching zero or more characters
        for i in ti..=text.len() {
            if glob_match_inner(pattern, text, pi + 1, i) {
                return true;
            }
        }
        return false;
    }
    if ti < text.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
        return glob_match_inner(pattern, text, pi + 1, ti + 1);
    }
    false
}

/// Convenience function to evaluate a PulseLang expression string.
pub fn eval_str(input: &str) -> Result<Value> {
    let parser = crate::lang::parser::Parser::new(input)?;
    let expr = parser.parse()?;
    let mut env = Env::new();
    eval(&expr, &mut env)
}

/// Evaluate with a given environment (for multi-line sessions).
pub fn eval_str_with_env(input: &str, env: &mut Env) -> Result<Value> {
    let parser = crate::lang::parser::Parser::new(input)?;
    let expr = parser.parse()?;
    eval(&expr, env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_literal() {
        assert_eq!(eval_str("42").unwrap(), Value::Int(42));
    }

    #[test]
    fn test_float_literal() {
        assert_eq!(eval_str("3.14").unwrap(), Value::Float(3.14));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(eval_str("\"hello\"").unwrap(), Value::Str("hello".into()));
    }

    #[test]
    fn test_bool_literal() {
        assert_eq!(eval_str("true").unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_addition() {
        assert_eq!(eval_str("2 + 3").unwrap(), Value::Int(5));
    }

    #[test]
    fn test_subtraction() {
        assert_eq!(eval_str("10 - 3").unwrap(), Value::Int(7));
    }

    #[test]
    fn test_multiplication() {
        assert_eq!(eval_str("4 * 5").unwrap(), Value::Int(20));
    }

    #[test]
    fn test_division() {
        assert_eq!(eval_str("10 % 4").unwrap(), Value::Float(2.5));
    }

    #[test]
    fn test_right_to_left() {
        // 2 * 3 + 4 => 2 * 7 = 14
        assert_eq!(eval_str("2 * 3 + 4").unwrap(), Value::Int(14));
    }

    #[test]
    fn test_parens() {
        // (2 * 3) + 4 = 10
        assert_eq!(eval_str("(2 * 3) + 4").unwrap(), Value::Int(10));
    }

    #[test]
    fn test_vector_add() {
        let result = eval_str("1 2 3 + 10 20 30").unwrap();
        assert_eq!(result, Value::IntVec(vec![11, 22, 33]));
    }

    #[test]
    fn test_scalar_broadcast() {
        let result = eval_str("1 2 3 + 10").unwrap();
        assert_eq!(result, Value::IntVec(vec![11, 12, 13]));
    }

    #[test]
    fn test_comparison() {
        let result = eval_str("1 2 3 > 2").unwrap();
        assert_eq!(result, Value::BoolVec(vec![false, false, true]));
    }

    #[test]
    fn test_assignment_and_use() {
        let mut env = Env::new();
        eval_str_with_env("x: 42", &mut env).unwrap();
        let result = eval_str_with_env("x + 1", &mut env).unwrap();
        assert_eq!(result, Value::Int(43));
    }

    #[test]
    fn test_sum() {
        assert_eq!(eval_str("sum 1 2 3 4").unwrap(), Value::Float(10.0));
    }

    #[test]
    fn test_avg() {
        assert_eq!(eval_str("avg 1 2 3 4").unwrap(), Value::Float(2.5));
    }

    #[test]
    fn test_min_max() {
        assert_eq!(eval_str("min 5 1 9 3").unwrap(), Value::Float(1.0));
        assert_eq!(eval_str("max 5 1 9 3").unwrap(), Value::Float(9.0));
    }

    #[test]
    fn test_count() {
        assert_eq!(eval_str("count 10 20 30").unwrap(), Value::Int(3));
    }

    #[test]
    fn test_first_last() {
        assert_eq!(eval_str("first 10 20 30").unwrap(), Value::Int(10));
        assert_eq!(eval_str("last 10 20 30").unwrap(), Value::Int(30));
    }

    #[test]
    fn test_sums() {
        let result = eval_str("sums 1 2 3 4").unwrap();
        assert_eq!(result, Value::FloatVec(vec![1.0, 3.0, 6.0, 10.0]));
    }

    #[test]
    fn test_til() {
        assert_eq!(eval_str("til 5").unwrap(), Value::IntVec(vec![0, 1, 2, 3, 4]));
    }

    #[test]
    fn test_rev() {
        assert_eq!(eval_str("rev 1 2 3").unwrap(), Value::IntVec(vec![3, 2, 1]));
    }

    #[test]
    fn test_asc_desc() {
        assert_eq!(eval_str("asc 3 1 4 1 5").unwrap(), Value::IntVec(vec![1, 1, 3, 4, 5]));
        assert_eq!(eval_str("desc 3 1 4 1 5").unwrap(), Value::IntVec(vec![5, 4, 3, 1, 1]));
    }

    #[test]
    fn test_distinct() {
        assert_eq!(eval_str("distinct 1 2 1 3 2").unwrap(), Value::IntVec(vec![1, 2, 3]));
    }

    #[test]
    fn test_where_bool() {
        assert_eq!(eval_str("where 10010b").unwrap(), Value::IntVec(vec![0, 3]));
    }

    #[test]
    fn test_take() {
        assert_eq!(eval_str("3 # 1 2 3 4 5").unwrap(), Value::IntVec(vec![1, 2, 3]));
    }

    #[test]
    fn test_drop() {
        assert_eq!(eval_str("2 _ 1 2 3 4 5").unwrap(), Value::IntVec(vec![3, 4, 5]));
    }

    #[test]
    fn test_join() {
        assert_eq!(
            eval_str("1 2 3 , 4 5").unwrap(),
            Value::IntVec(vec![1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn test_neg() {
        assert_eq!(eval_str("neg 3").unwrap(), Value::Int(-3));
    }

    #[test]
    fn test_not() {
        assert_eq!(eval_str("not true").unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_abs() {
        assert_eq!(eval_str("abs -5").unwrap(), Value::Float(5.0));
    }

    #[test]
    fn test_lambda() {
        let mut env = Env::new();
        eval_str_with_env("double: {x * 2}", &mut env).unwrap();
        let result = eval_str_with_env("double[21]", &mut env).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_lambda_named_params() {
        let mut env = Env::new();
        eval_str_with_env("add: {[a;b] a + b}", &mut env).unwrap();
        let result = eval_str_with_env("add[3; 4]", &mut env).unwrap();
        assert_eq!(result, Value::Int(7));
    }

    #[test]
    fn test_conditional() {
        let mut env = Env::new();
        eval_str_with_env("x: 5", &mut env).unwrap();
        let result = eval_str_with_env("$[x > 3; 1; 0]", &mut env).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_pipeline() {
        assert_eq!(eval_str("1 2 3 4 |> sum").unwrap(), Value::Float(10.0));
    }

    #[test]
    fn test_deltas() {
        let result = eval_str("deltas 10 13 17 22").unwrap();
        assert_eq!(result, Value::FloatVec(vec![10.0, 3.0, 4.0, 5.0]));
    }

    #[test]
    fn test_med() {
        assert_eq!(eval_str("med 1 5 3 9 2").unwrap(), Value::Float(3.0));
    }

    #[test]
    fn test_dev() {
        let result = eval_str("dev 2 4 4 4 6").unwrap();
        if let Value::Float(v) = result {
            assert!((v - 1.2649).abs() < 0.001);
        } else {
            panic!("expected float");
        }
    }

    #[test]
    fn test_dict() {
        let result = eval_str("`a`b ! 1 2").unwrap();
        if let Value::Dict(d) = result {
            assert_eq!(d.get("a").unwrap(), &Value::Int(1));
            assert_eq!(d.get("b").unwrap(), &Value::Int(2));
        } else {
            panic!("expected dict");
        }
    }

    #[test]
    fn test_indexing() {
        let mut env = Env::new();
        eval_str_with_env("x: 10 20 30 40 50", &mut env).unwrap();
        assert_eq!(eval_str_with_env("x[0]", &mut env).unwrap(), Value::Int(10));
        assert_eq!(eval_str_with_env("x[-1]", &mut env).unwrap(), Value::Int(50));
    }

    #[test]
    fn test_bool_indexing() {
        let mut env = Env::new();
        eval_str_with_env("x: 10 20 30 40 50", &mut env).unwrap();
        let result = eval_str_with_env("x[10010b]", &mut env).unwrap();
        assert_eq!(result, Value::IntVec(vec![10, 40]));
    }

    #[test]
    fn test_type_fn() {
        assert_eq!(eval_str("type 42").unwrap(), Value::Symbol("int".into()));
        assert_eq!(eval_str("type 3.14").unwrap(), Value::Symbol("float".into()));
    }

    #[test]
    fn test_string_ops() {
        assert_eq!(eval_str("upper \"hello\"").unwrap(), Value::Str("HELLO".into()));
        assert_eq!(eval_str("lower \"HELLO\"").unwrap(), Value::Str("hello".into()));
    }

    #[test]
    fn test_prev_next() {
        let result = eval_str("prev 10 20 30").unwrap();
        assert_eq!(result, Value::IntVec(vec![0, 10, 20]));
        let result = eval_str("next 10 20 30").unwrap();
        assert_eq!(result, Value::IntVec(vec![20, 30, 0]));
    }

    #[test]
    fn test_xbar() {
        let mut env = Env::new();
        eval_str_with_env("ts: 0 5 10 15 20 25", &mut env).unwrap();
        let result = eval_str_with_env("xbar[10; ts]", &mut env).unwrap();
        assert_eq!(result, Value::IntVec(vec![0, 0, 10, 10, 20, 20]));
    }

    #[test]
    fn test_mavg() {
        let mut env = Env::new();
        eval_str_with_env("v: 1.0 2.0 3.0 4.0 5.0", &mut env).unwrap();
        let result = eval_str_with_env("mavg[3; v]", &mut env).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 5);
            assert!((r[0] - 1.0).abs() < 0.001);
            assert!((r[1] - 1.5).abs() < 0.001);
            assert!((r[2] - 2.0).abs() < 0.001);
            assert!((r[3] - 3.0).abs() < 0.001);
            assert!((r[4] - 4.0).abs() < 0.001);
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_ema() {
        let mut env = Env::new();
        eval_str_with_env("v: 1.0 2.0 3.0 4.0", &mut env).unwrap();
        let result = eval_str_with_env("ema[0.5; v]", &mut env).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 4);
            assert!((r[0] - 1.0).abs() < 0.001);
            assert!((r[1] - 1.5).abs() < 0.001);
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_list() {
        let result = eval_str("(1; \"a\"; `x)").unwrap();
        assert_eq!(
            result,
            Value::List(vec![Value::Int(1), Value::Str("a".into()), Value::Symbol("x".into())])
        );
    }

    #[test]
    fn test_symbol_vec() {
        let result = eval_str("`a`b`c").unwrap();
        assert_eq!(result, Value::SymVec(vec!["a".into(), "b".into(), "c".into()]));
    }

    #[test]
    fn test_in_operator() {
        assert_eq!(eval_str("2 in 1 2 3").unwrap(), Value::Bool(true));
        assert_eq!(eval_str("5 in 1 2 3").unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_duration() {
        assert_eq!(eval_str("5m").unwrap(), Value::Duration(300_000_000_000));
    }

    #[test]
    fn test_duration_add() {
        assert_eq!(eval_str("5m + 30s").unwrap(), Value::Duration(330_000_000_000));
    }

    #[test]
    fn test_match() {
        assert_eq!(eval_str("1 2 3 ~ 1 2 3").unwrap(), Value::Bool(true));
    }

    #[test]
    fn test_multi_statement_env() {
        let mut env = Env::new();
        eval_str_with_env("a: 10", &mut env).unwrap();
        eval_str_with_env("b: 20", &mut env).unwrap();
        let result = eval_str_with_env("a + b", &mut env).unwrap();
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_timestamp_member_week_dow() {
        // 2024.01.15 is a Monday, ISO week 3
        let mut env = Env::new();
        eval_str_with_env("t: 2024.01.15D12:00:00", &mut env).unwrap();
        assert_eq!(eval_str_with_env("t.week", &mut env).unwrap(), Value::Int(3));
        assert_eq!(eval_str_with_env("t.dow", &mut env).unwrap(), Value::Int(0)); // Monday
    }

    #[test]
    fn test_timestamp_vec_member_extraction() {
        // 2024.01.15 = Monday ISO week 3, 2024.06.20 = Thursday ISO week 25
        let mut env = Env::new();
        let ts1 = if let Value::Timestamp(ns) = eval_str("2024.01.15D12:30:45").unwrap() { ns } else { panic!() };
        let ts2 = if let Value::Timestamp(ns) = eval_str("2024.06.20D09:15:30").unwrap() { ns } else { panic!() };
        env.set("ts".into(), Value::TimestampVec(vec![ts1, ts2]));
        assert_eq!(eval_str_with_env("ts.year", &mut env).unwrap(), Value::IntVec(vec![2024, 2024]));
        assert_eq!(eval_str_with_env("ts.month", &mut env).unwrap(), Value::IntVec(vec![1, 6]));
        assert_eq!(eval_str_with_env("ts.day", &mut env).unwrap(), Value::IntVec(vec![15, 20]));
        assert_eq!(eval_str_with_env("ts.hour", &mut env).unwrap(), Value::IntVec(vec![12, 9]));
        assert_eq!(eval_str_with_env("ts.minute", &mut env).unwrap(), Value::IntVec(vec![30, 15]));
        assert_eq!(eval_str_with_env("ts.second", &mut env).unwrap(), Value::IntVec(vec![45, 30]));
        assert_eq!(eval_str_with_env("ts.week", &mut env).unwrap(), Value::IntVec(vec![3, 25]));
        assert_eq!(eval_str_with_env("ts.dow", &mut env).unwrap(), Value::IntVec(vec![0, 3])); // Mon, Thu
    }

    #[test]
    fn test_mdev() {
        let mut env = Env::new();
        eval_str_with_env("v: 2.0 4.0 4.0 4.0 6.0", &mut env).unwrap();
        let result = eval_str_with_env("mdev[3; v]", &mut env).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 5);
            // Window of 1: dev of [2] = 0
            assert!((r[0] - 0.0).abs() < 0.001);
            // Window of 2: dev of [2,4] = 1.0
            assert!((r[1] - 1.0).abs() < 0.001);
            // Window of 3: dev of [2,4,4] = 0.9428
            assert!((r[2] - 0.9428).abs() < 0.001);
            // Window of 3: dev of [4,4,4] = 0
            assert!((r[3] - 0.0).abs() < 0.001);
            // Window of 3: dev of [4,4,6] = 0.9428
            assert!((r[4] - 0.9428).abs() < 0.001);
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_mcount() {
        let result = eval_builtin_fn(
            "mcount",
            &[Value::Int(3), Value::FloatVec(vec![1.0, f64::NAN, 3.0, 4.0, f64::NAN])],
        ).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 5);
            assert!((r[0] - 1.0).abs() < 0.001); // [1.0] -> 1 non-NaN
            assert!((r[1] - 1.0).abs() < 0.001); // [1.0, NaN] -> 1 non-NaN
            assert!((r[2] - 2.0).abs() < 0.001); // [1.0, NaN, 3.0] -> 2 non-NaN
            assert!((r[3] - 2.0).abs() < 0.001); // [NaN, 3.0, 4.0] -> 2 non-NaN
            assert!((r[4] - 2.0).abs() < 0.001); // [3.0, 4.0, NaN] -> 2 non-NaN
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_mcount_all_valid() {
        let mut env = Env::new();
        eval_str_with_env("v: 1.0 2.0 3.0 4.0 5.0", &mut env).unwrap();
        let result = eval_str_with_env("mcount[3; v]", &mut env).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 5);
            assert!((r[0] - 1.0).abs() < 0.001);
            assert!((r[1] - 2.0).abs() < 0.001);
            assert!((r[2] - 3.0).abs() < 0.001);
            assert!((r[3] - 3.0).abs() < 0.001);
            assert!((r[4] - 3.0).abs() < 0.001);
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_wma() {
        let mut env = Env::new();
        eval_str_with_env("v: 1.0 2.0 3.0 4.0 5.0", &mut env).unwrap();
        let result = eval_str_with_env("wma[3; v]", &mut env).unwrap();
        if let Value::FloatVec(r) = result {
            assert_eq!(r.len(), 5);
            // i=0: window [1.0], weights [1] => 1.0/1 = 1.0
            assert!((r[0] - 1.0).abs() < 0.001);
            // i=1: window [1.0, 2.0], weights [1,2] => (1+4)/3 = 5/3
            assert!((r[1] - 5.0 / 3.0).abs() < 0.001);
            // i=2: window [1.0, 2.0, 3.0], weights [1,2,3] => (1+4+9)/6 = 14/6
            assert!((r[2] - 14.0 / 6.0).abs() < 0.001);
            // i=3: window [2.0, 3.0, 4.0], weights [1,2,3] => (2+6+12)/6 = 20/6
            assert!((r[3] - 20.0 / 6.0).abs() < 0.001);
            // i=4: window [3.0, 4.0, 5.0], weights [1,2,3] => (3+8+15)/6 = 26/6
            assert!((r[4] - 26.0 / 6.0).abs() < 0.001);
        } else {
            panic!("expected float vec");
        }
    }

    #[test]
    fn test_resample() {
        let result = eval_builtin_fn(
            "resample",
            &[
                Value::Int(10),
                Value::IntVec(vec![0, 5, 10, 15, 20, 25]),
                Value::FloatVec(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
                Value::BuiltinFn("avg".to_string()),
            ],
        ).unwrap();
        if let Value::Table(t) = result {
            assert_eq!(t.columns, vec!["ts", "value"]);
            let ts = t.data.get("ts").unwrap();
            let vals = t.data.get("value").unwrap();
            assert_eq!(ts, &Value::IntVec(vec![0, 10, 20]));
            if let Value::FloatVec(v) = vals {
                assert!((v[0] - 1.5).abs() < 0.001);  // avg(1,2)
                assert!((v[1] - 3.5).abs() < 0.001);  // avg(3,4)
                assert!((v[2] - 5.5).abs() < 0.001);  // avg(5,6)
            } else {
                panic!("expected float vec for values");
            }
        } else {
            panic!("expected table");
        }
    }

    #[test]
    fn test_asof() {
        let result = eval_builtin_fn(
            "asof",
            &[
                Value::IntVec(vec![1, 3, 5, 7]),
                Value::FloatVec(vec![10.0, 30.0, 50.0, 70.0]),
                Value::IntVec(vec![0, 2, 4, 6]),
                Value::FloatVec(vec![100.0, 200.0, 300.0, 400.0]),
            ],
        ).unwrap();
        if let Value::Table(t) = result {
            assert_eq!(t.columns, vec!["ts", "left", "right"]);
            let ts = t.data.get("ts").unwrap();
            let left = t.data.get("left").unwrap();
            let right = t.data.get("right").unwrap();
            assert_eq!(ts, &Value::IntVec(vec![1, 3, 5, 7]));
            assert_eq!(left, &Value::FloatVec(vec![10.0, 30.0, 50.0, 70.0]));
            if let Value::FloatVec(r) = right {
                // ts=1: last right where tr<=1 is tr=0 -> 100.0
                assert!((r[0] - 100.0).abs() < 0.001);
                // ts=3: last right where tr<=3 is tr=2 -> 200.0
                assert!((r[1] - 200.0).abs() < 0.001);
                // ts=5: last right where tr<=5 is tr=4 -> 300.0
                assert!((r[2] - 300.0).abs() < 0.001);
                // ts=7: last right where tr<=7 is tr=6 -> 400.0
                assert!((r[3] - 400.0).abs() < 0.001);
            } else {
                panic!("expected float vec for right");
            }
        } else {
            panic!("expected table");
        }
    }

    #[test]
    fn test_asof_no_match() {
        let result = eval_builtin_fn(
            "asof",
            &[
                Value::IntVec(vec![0]),
                Value::FloatVec(vec![10.0]),
                Value::IntVec(vec![5, 10]),
                Value::FloatVec(vec![100.0, 200.0]),
            ],
        ).unwrap();
        if let Value::Table(t) = result {
            if let Value::FloatVec(r) = t.data.get("right").unwrap() {
                assert!(r[0].is_nan()); // no right ts <= 0
            } else {
                panic!("expected float vec");
            }
        } else {
            panic!("expected table");
        }
    }
}
