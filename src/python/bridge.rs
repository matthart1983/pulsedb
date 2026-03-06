//! Bridge between Viper (Python interpreter) and PulseDB.
//!
//! Uses thread-local storage to give Viper native functions access to the database,
//! since Viper's native function signature is a plain `fn` pointer without closures.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Result;

use crate::engine::Database;
use crate::lang::value::Value as PulseValue;
use crate::model::{DataPoint, FieldValue, Tags};

use viper::bytecode::Value as ViperValue;
use viper::interpreter::Interpreter;
use viper::lexer::Lexer;
use viper::parser::Parser;
use viper::symbol::Interner;
use viper::vm::VM;

thread_local! {
    static DB_HANDLE: RefCell<Option<Arc<Database>>> = RefCell::new(None);
}

fn with_db<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce(&Database) -> Result<R, String>,
{
    DB_HANDLE.with(|cell| {
        let borrow = cell.borrow();
        let db = borrow.as_ref().ok_or("no database connection")?;
        f(db)
    })
}

/// Native function: db_query(expr_string) -> string
///
/// Evaluates a PulseLang expression against the database and returns
/// the result as a string.
fn native_db_query(args: &[ViperValue]) -> Result<ViperValue, String> {
    if args.len() != 1 {
        return Err(format!("db_query() takes 1 argument, got {}", args.len()));
    }
    let expr = match &args[0] {
        ViperValue::String(s) => s.as_str().to_string(),
        other => return Err(format!("db_query() argument must be a string, got {other}")),
    };

    with_db(|db| {
        let result = db.query_lang(&expr).map_err(|e| e.to_string())?;
        Ok(pulse_value_to_viper(&result))
    })
}

/// Native function: db_insert(measurement, fields_dict, [tags_dict], [timestamp_ns])
///
/// Inserts a data point into the database.
fn native_db_insert(args: &[ViperValue]) -> Result<ViperValue, String> {
    if args.len() < 2 || args.len() > 4 {
        return Err(format!(
            "db_insert() takes 2-4 arguments (measurement, fields, [tags], [timestamp]), got {}",
            args.len()
        ));
    }

    let measurement = match &args[0] {
        ViperValue::String(s) => s.as_str().to_string(),
        other => return Err(format!("db_insert() measurement must be a string, got {other}")),
    };

    let fields = viper_dict_to_fields(&args[1])?;

    let tags = if args.len() >= 3 {
        viper_dict_to_tags(&args[2])?
    } else {
        Tags::new()
    };

    let timestamp = if args.len() >= 4 {
        match &args[3] {
            ViperValue::Integer(n) => *n,
            ViperValue::Float(f) => *f as i64,
            other => return Err(format!("db_insert() timestamp must be a number, got {other}")),
        }
    } else {
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    };

    let point = DataPoint {
        measurement,
        tags,
        fields,
        timestamp,
    };

    with_db(|db| {
        db.write(vec![point]).map_err(|e| e.to_string())?;
        Ok(ViperValue::None)
    })
}

/// Native function: db_measurements() -> list of strings
///
/// Returns all known measurement names.
fn native_db_measurements(args: &[ViperValue]) -> Result<ViperValue, String> {
    if !args.is_empty() {
        return Err(format!(
            "db_measurements() takes 0 arguments, got {}",
            args.len()
        ));
    }

    with_db(|db| {
        let names = db.measurement_names();
        let list: Vec<ViperValue> = names
            .into_iter()
            .map(|s| ViperValue::String(Rc::new(s)))
            .collect();
        Ok(ViperValue::List(Rc::new(list)))
    })
}

/// Native function: db_fields(measurement) -> list of strings
///
/// Returns field names for a measurement.
fn native_db_fields(args: &[ViperValue]) -> Result<ViperValue, String> {
    if args.len() != 1 {
        return Err(format!("db_fields() takes 1 argument, got {}", args.len()));
    }
    let measurement = match &args[0] {
        ViperValue::String(s) => s.as_str().to_string(),
        other => return Err(format!("db_fields() argument must be a string, got {other}")),
    };

    with_db(|db| {
        let names = db.field_names(&measurement);
        let list: Vec<ViperValue> = names
            .into_iter()
            .map(|s| ViperValue::String(Rc::new(s)))
            .collect();
        Ok(ViperValue::List(Rc::new(list)))
    })
}

/// Convert a PulseLang Value to a Viper Value.
fn pulse_value_to_viper(val: &PulseValue) -> ViperValue {
    match val {
        PulseValue::Int(n) => ViperValue::Integer(*n),
        PulseValue::UInt(n) => ViperValue::Integer(*n as i64),
        PulseValue::Float(f) => ViperValue::Float(*f),
        PulseValue::Bool(b) => ViperValue::Boolean(*b),
        PulseValue::Str(s) => ViperValue::String(Rc::new(s.clone())),
        PulseValue::Symbol(s) => ViperValue::String(Rc::new(s.clone())),
        PulseValue::Null => ViperValue::None,
        PulseValue::Timestamp(ns) => ViperValue::Integer(*ns),
        PulseValue::Duration(ns) => ViperValue::Integer(*ns),

        PulseValue::IntVec(v) => {
            let items = v.iter().map(|n| ViperValue::Integer(*n)).collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::FloatVec(v) => {
            let items = v.iter().map(|f| ViperValue::Float(*f)).collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::BoolVec(v) => {
            let items = v.iter().map(|b| ViperValue::Boolean(*b)).collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::SymVec(v) | PulseValue::StrVec(v) => {
            let items = v
                .iter()
                .map(|s| ViperValue::String(Rc::new(s.clone())))
                .collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::TimestampVec(v) => {
            let items = v.iter().map(|n| ViperValue::Integer(*n)).collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::List(v) => {
            let items = v.iter().map(pulse_value_to_viper).collect();
            ViperValue::List(Rc::new(items))
        }
        PulseValue::Dict(d) => {
            let pairs = d
                .iter()
                .map(|(k, v)| {
                    (
                        ViperValue::String(Rc::new(k.clone())),
                        pulse_value_to_viper(v),
                    )
                })
                .collect();
            ViperValue::Dict(pairs)
        }
        PulseValue::Table(table) => {
            let pairs = table
                .columns
                .iter()
                .filter_map(|col| {
                    table.data.get(col).map(|v| {
                        (
                            ViperValue::String(Rc::new(col.clone())),
                            pulse_value_to_viper(v),
                        )
                    })
                })
                .collect();
            ViperValue::Dict(pairs)
        }
        PulseValue::Lambda { .. } | PulseValue::BuiltinFn(_) => {
            ViperValue::String(Rc::new(format!("{val}")))
        }
    }
}

/// Convert a Viper Dict value to PulseDB field map.
fn viper_dict_to_fields(val: &ViperValue) -> Result<BTreeMap<String, FieldValue>, String> {
    let pairs = match val {
        ViperValue::Dict(pairs) => pairs,
        other => return Err(format!("expected dict for fields, got {other}")),
    };
    let mut fields = BTreeMap::new();
    for (k, v) in pairs {
        let key = match k {
            ViperValue::String(s) => s.as_str().to_string(),
            other => return Err(format!("field key must be a string, got {other}")),
        };
        let fv = match v {
            ViperValue::Integer(n) => FieldValue::Integer(*n),
            ViperValue::Float(f) => FieldValue::Float(*f),
            ViperValue::Boolean(b) => FieldValue::Boolean(*b),
            ViperValue::String(s) => FieldValue::String(s.as_str().to_string()),
            other => return Err(format!("unsupported field value type: {other}")),
        };
        fields.insert(key, fv);
    }
    Ok(fields)
}

/// Convert a Viper Dict value to PulseDB tag map.
fn viper_dict_to_tags(val: &ViperValue) -> Result<Tags, String> {
    let pairs = match val {
        ViperValue::Dict(pairs) => pairs,
        other => return Err(format!("expected dict for tags, got {other}")),
    };
    let mut tags = Tags::new();
    for (k, v) in pairs {
        let key = match k {
            ViperValue::String(s) => s.as_str().to_string(),
            other => return Err(format!("tag key must be a string, got {other}")),
        };
        let val = match v {
            ViperValue::String(s) => s.as_str().to_string(),
            other => format!("{other}"),
        };
        tags.insert(key, val);
    }
    Ok(tags)
}

/// Register all PulseDB native functions into a Viper VM.
fn register_builtins_vm(vm: &mut VM) {
    let interner = vm.interner_mut();

    let query_sym = interner.intern("db_query");
    let insert_sym = interner.intern("db_insert");
    let measurements_sym = interner.intern("db_measurements");
    let fields_sym = interner.intern("db_fields");

    vm.set_global(
        query_sym,
        ViperValue::NativeFunction {
            name: Rc::new("db_query".to_string()),
            func: native_db_query,
        },
    );
    vm.set_global(
        insert_sym,
        ViperValue::NativeFunction {
            name: Rc::new("db_insert".to_string()),
            func: native_db_insert,
        },
    );
    vm.set_global(
        measurements_sym,
        ViperValue::NativeFunction {
            name: Rc::new("db_measurements".to_string()),
            func: native_db_measurements,
        },
    );
    vm.set_global(
        fields_sym,
        ViperValue::NativeFunction {
            name: Rc::new("db_fields".to_string()),
            func: native_db_fields,
        },
    );
}

/// Register all PulseDB native functions into a Viper tree-walking interpreter.
fn register_builtins_interp(interp: &mut Interpreter) {
    let interner = interp.interner_mut();

    let query_sym = interner.intern("db_query");
    let insert_sym = interner.intern("db_insert");
    let measurements_sym = interner.intern("db_measurements");
    let fields_sym = interner.intern("db_fields");

    // The interpreter uses its own Value type from viper::interpreter
    use viper::interpreter::Value as InterpValue;

    interp.set_global(
        query_sym,
        InterpValue::NativeFunction {
            name: "db_query".to_string(),
            func: native_db_query_interp,
        },
    );
    interp.set_global(
        insert_sym,
        InterpValue::NativeFunction {
            name: "db_insert".to_string(),
            func: native_db_insert_interp,
        },
    );
    interp.set_global(
        measurements_sym,
        InterpValue::NativeFunction {
            name: "db_measurements".to_string(),
            func: native_db_measurements_interp,
        },
    );
    interp.set_global(
        fields_sym,
        InterpValue::NativeFunction {
            name: "db_fields".to_string(),
            func: native_db_fields_interp,
        },
    );
}

// Interpreter-side native functions (use viper::interpreter::Value)
use viper::interpreter::Value as InterpValue;

fn interp_to_bytecode_val(val: &InterpValue) -> ViperValue {
    match val {
        InterpValue::Integer(n) => ViperValue::Integer(*n),
        InterpValue::Float(f) => ViperValue::Float(*f),
        InterpValue::String(s) => ViperValue::String(Rc::new(s.clone())),
        InterpValue::Boolean(b) => ViperValue::Boolean(*b),
        InterpValue::List(items) => {
            let v: Vec<ViperValue> = items.iter().map(interp_to_bytecode_val).collect();
            ViperValue::List(Rc::new(v))
        }
        InterpValue::Dict(pairs) => {
            let p: Vec<(ViperValue, ViperValue)> = pairs
                .iter()
                .map(|(k, v)| (interp_to_bytecode_val(k), interp_to_bytecode_val(v)))
                .collect();
            ViperValue::Dict(p)
        }
        InterpValue::None => ViperValue::None,
        InterpValue::Function(_) | InterpValue::NativeFunction { .. } => ViperValue::None,
    }
}

fn bytecode_to_interp_val(val: &ViperValue) -> InterpValue {
    match val {
        ViperValue::Integer(n) => InterpValue::Integer(*n),
        ViperValue::Float(f) => InterpValue::Float(*f),
        ViperValue::String(s) => InterpValue::String(s.as_str().to_string()),
        ViperValue::Boolean(b) => InterpValue::Boolean(*b),
        ViperValue::List(items) => {
            InterpValue::List(items.iter().map(bytecode_to_interp_val).collect())
        }
        ViperValue::Dict(pairs) => InterpValue::Dict(
            pairs
                .iter()
                .map(|(k, v)| (bytecode_to_interp_val(k), bytecode_to_interp_val(v)))
                .collect(),
        ),
        ViperValue::None => InterpValue::None,
        ViperValue::Function(_) | ViperValue::NativeFunction { .. } => InterpValue::None,
    }
}

fn native_db_query_interp(args: &[InterpValue]) -> Result<InterpValue, String> {
    let bc_args: Vec<ViperValue> = args.iter().map(interp_to_bytecode_val).collect();
    let result = native_db_query(&bc_args)?;
    Ok(bytecode_to_interp_val(&result))
}

fn native_db_insert_interp(args: &[InterpValue]) -> Result<InterpValue, String> {
    let bc_args: Vec<ViperValue> = args.iter().map(interp_to_bytecode_val).collect();
    let result = native_db_insert(&bc_args)?;
    Ok(bytecode_to_interp_val(&result))
}

fn native_db_measurements_interp(args: &[InterpValue]) -> Result<InterpValue, String> {
    let bc_args: Vec<ViperValue> = args.iter().map(interp_to_bytecode_val).collect();
    let result = native_db_measurements(&bc_args)?;
    Ok(bytecode_to_interp_val(&result))
}

fn native_db_fields_interp(args: &[InterpValue]) -> Result<InterpValue, String> {
    let bc_args: Vec<ViperValue> = args.iter().map(interp_to_bytecode_val).collect();
    let result = native_db_fields(&bc_args)?;
    Ok(bytecode_to_interp_val(&result))
}

/// Execute Python code against a PulseDB database and return captured output lines.
///
/// Used by the HTTP API to run Python from the web UI.
pub fn exec_python_code(db: &Arc<Database>, code: &str) -> Result<Vec<String>, String> {
    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = Some(db.clone());
    });

    let mut interner = Interner::new();
    let mut lexer = Lexer::new(code);
    let tokens = lexer.tokenize()?;
    let stmts = {
        let mut parser = Parser::new(tokens, &mut interner);
        parser.parse()?
    };

    let code_obj = viper::compiler::compile_module(&stmts, &mut interner);
    let mut vm = VM::new(interner);
    vm.set_suppress_output(true);
    register_builtins_vm(&mut vm);
    vm.run(&code_obj)?;

    let output = vm.get_output().to_vec();

    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = None;
    });

    Ok(output)
}

/// Run a Python script file against a PulseDB database using the bytecode VM.
pub fn run_python_file(db: Arc<Database>, path: &Path) -> Result<()> {
    let code = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read file {}: {e}", path.display()))?;

    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = Some(db);
    });

    let mut interner = Interner::new();
    let mut lexer = Lexer::new(&code);
    let tokens = lexer.tokenize().map_err(|e| anyhow::anyhow!("{e}"))?;
    let stmts = {
        let mut parser = Parser::new(tokens, &mut interner);
        parser.parse().map_err(|e| anyhow::anyhow!("{e}"))?
    };

    let code_obj = viper::compiler::compile_module(&stmts, &mut interner);
    let mut vm = VM::new(interner);
    register_builtins_vm(&mut vm);
    vm.run(&code_obj).map_err(|e| anyhow::anyhow!("{e}"))?;

    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = None;
    });

    Ok(())
}

/// Run a Python expression string against a PulseDB database.
pub fn run_python_expr(db: Arc<Database>, expr: &str) -> Result<()> {
    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = Some(db);
    });

    let mut interner = Interner::new();
    let mut lexer = Lexer::new(expr);
    let tokens = lexer.tokenize().map_err(|e| anyhow::anyhow!("{e}"))?;
    let stmts = {
        let mut parser = Parser::new(tokens, &mut interner);
        parser.parse().map_err(|e| anyhow::anyhow!("{e}"))?
    };

    let code_obj = viper::compiler::compile_module(&stmts, &mut interner);
    let mut vm = VM::new(interner);
    register_builtins_vm(&mut vm);
    vm.run(&code_obj).map_err(|e| anyhow::anyhow!("{e}"))?;

    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = None;
    });

    Ok(())
}

/// Run an interactive Python REPL against a PulseDB database.
pub fn run_python_repl(db: Arc<Database>) -> Result<()> {
    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = Some(db);
    });

    println!(
        "PulseDB Python v{} — Viper interactive shell",
        env!("CARGO_PKG_VERSION")
    );
    println!("Built-in functions: db_query(), db_insert(), db_measurements(), db_fields()");
    println!("Type Python code. Ctrl-D to exit.\n");

    let stdin = io::stdin();
    let mut interp = Interpreter::new(Interner::new());
    register_builtins_interp(&mut interp);

    loop {
        print!("py> ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                println!();
                break;
            }
            Ok(_) => {
                if line.trim().is_empty() {
                    continue;
                }

                // Collect multi-line input for blocks
                if line.trim_end().ends_with(':') {
                    let mut block = line.clone();
                    loop {
                        print!("... ");
                        io::stdout().flush().unwrap();
                        let mut next_line = String::new();
                        match stdin.lock().read_line(&mut next_line) {
                            Ok(0) => break,
                            Ok(_) => {
                                if next_line.trim().is_empty() {
                                    break;
                                }
                                block.push_str(&next_line);
                            }
                            Err(e) => {
                                eprintln!("Error: {e}");
                                break;
                            }
                        }
                    }
                    line = block;
                }

                let mut lexer = Lexer::new(&line);
                let tokens = match lexer.tokenize() {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("SyntaxError: {e}");
                        continue;
                    }
                };
                let stmts = {
                    let mut parser = Parser::new(tokens, interp.interner_mut());
                    match parser.parse() {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("SyntaxError: {e}");
                            continue;
                        }
                    }
                };
                if let Err(e) = interp.run(&stmts) {
                    eprintln!("Error: {e}");
                }
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    DB_HANDLE.with(|cell| {
        *cell.borrow_mut() = None;
    });

    Ok(())
}
