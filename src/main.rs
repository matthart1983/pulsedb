use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use pulsedb::{cli, engine, server, storage};

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
            retention,
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
                retention_secs: retention,
                ..Default::default()
            };

            let retention_secs = config.retention_secs;
            let data_dir_maint = config.data_dir.clone();

            let db = Arc::new(engine::Database::open(config)?);

            tracing::info!("PulseDB v{} starting", env!("CARGO_PKG_VERSION"));

            // Background maintenance (every 60 seconds)
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    interval.tick().await;

                    // Compaction
                    let compactor = storage::Compactor::new(&data_dir_maint);
                    match compactor.compact_all() {
                        Ok(stats) if stats.segments_removed > 0 => {
                            tracing::info!(removed = stats.segments_removed, "compaction complete");
                        }
                        Err(e) => tracing::warn!(error = %e, "compaction failed"),
                        _ => {}
                    }

                    // Retention
                    if retention_secs > 0 {
                        let policy = storage::RetentionPolicy::new(&data_dir_maint, retention_secs);
                        match policy.enforce() {
                            Ok(n) if n > 0 => {
                                tracing::info!(dropped = n, "retention policy enforced");
                            }
                            Err(e) => tracing::warn!(error = %e, "retention enforcement failed"),
                            _ => {}
                        }
                    }
                }
            });

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
