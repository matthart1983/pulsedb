use std::path::PathBuf;

use clap::{Parser, Subcommand};

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
        #[arg(long, default_value = "info")]
        log_level: String,
    },
    /// Show server version
    Version,
}
