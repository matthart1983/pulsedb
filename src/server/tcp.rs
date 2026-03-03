use std::sync::Arc;

use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::engine::Database;
use crate::model::DataPoint;
use crate::server::protocol;

pub async fn run_tcp_server(db: Arc<Database>, addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(addr, "TCP line protocol server listening");

    loop {
        let (stream, peer) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(db, stream).await {
                warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle_connection(
    db: Arc<Database>,
    stream: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    let reader = tokio::io::BufReader::new(stream);
    let mut lines = reader.lines();
    let mut batch: Vec<DataPoint> = Vec::new();
    let batch_size = 1000;

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match protocol::parse_line(&line) {
            Ok(mut point) => {
                if point.timestamp == 0 {
                    point.timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                }
                batch.push(point);
                if batch.len() >= batch_size {
                    db.write(std::mem::take(&mut batch))?;
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to parse line");
            }
        }
    }

    if !batch.is_empty() {
        db.write(batch)?;
    }

    Ok(())
}
