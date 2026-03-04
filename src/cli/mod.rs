use std::path::PathBuf;

use clap::{Parser, Subcommand};

pub mod repl;

#[derive(Parser, Debug)]
#[command(name = "pulsedb", version, about = "High-performance time-series database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the PulseDB server
    Server {
        #[arg(long, default_value = "./pulsedb_data")]
        data_dir: PathBuf,
        #[arg(long, default_value = "8086")]
        tcp_port: u16,
        #[arg(long, default_value = "8087")]
        http_port: u16,
        #[arg(long, default_value = "batch")]
        wal_fsync: String,
        #[arg(long, default_value = "67108864")]
        memtable_size: usize,
        #[arg(long, default_value = "3600")]
        segment_duration: u64,
        #[arg(long, default_value = "0")]
        retention: u64,
        #[arg(long, default_value = "info")]
        log_level: String,
    },
    /// PulseLang interactive REPL or script runner
    Lang {
        /// Data directory (database to query)
        #[arg(long, default_value = "./pulsedb_data")]
        data_dir: PathBuf,
        /// Execute a single expression and exit
        #[arg(short, long)]
        execute: Option<String>,
        /// Execute a .pulse script file
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Output format: text (default), json, csv
        #[arg(long, default_value = "text")]
        format: String,
    },
    /// Show server version
    Version,
}
