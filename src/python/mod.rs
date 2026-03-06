//! Python scripting support for PulseDB via the Viper interpreter.
//!
//! Provides a bridge between Viper's Python runtime and PulseDB's database engine,
//! injecting database builtins (`db_query`, `db_insert`, `db_measurements`) into
//! the Python global scope.

pub mod bridge;
