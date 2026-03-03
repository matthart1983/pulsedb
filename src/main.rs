pub mod cli;
pub mod encoding;
pub mod engine;
pub mod index;
pub mod model;
pub mod query;
pub mod server;
pub mod storage;

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();

    match cli.command {
        Some(cli::Commands::Server {
            data_dir,
            tcp_port,
            http_port,
            wal_fsync,
            memtable_size,
            segment_duration,
            log_level,
        }) => {
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new(&log_level))
                .init();

            let fsync = match wal_fsync.as_str() {
                "every" => engine::config::FsyncPolicy::Every,
                "none" => engine::config::FsyncPolicy::None,
                _ => engine::config::FsyncPolicy::Batch,
            };

            let config = engine::config::EngineConfig {
                data_dir,
                memtable_size_bytes: memtable_size,
                wal_fsync: fsync,
                segment_duration_secs: segment_duration,
                ..Default::default()
            };

            let db = Arc::new(engine::Database::open(config)?);

            tracing::info!("PulseDB v{} starting", env!("CARGO_PKG_VERSION"));

            let tcp_addr = format!("0.0.0.0:{tcp_port}");
            let http_addr = format!("0.0.0.0:{http_port}");

            let db_tcp = db.clone();
            let tcp_handle = tokio::spawn(async move {
                if let Err(e) = server::tcp::run_tcp_server(db_tcp, &tcp_addr).await {
                    tracing::error!(error = %e, "TCP server error");
                }
            });

            let db_http = db.clone();
            let http_handle = tokio::spawn(async move {
                if let Err(e) = server::http::run_http_server(db_http, &http_addr).await {
                    tracing::error!(error = %e, "HTTP server error");
                }
            });

            tokio::select! {
                _ = tcp_handle => {},
                _ = http_handle => {},
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutting down");
                }
            }
        }
        Some(cli::Commands::Version) | None => {
            println!("PulseDB v{}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}
