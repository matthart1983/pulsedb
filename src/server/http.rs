use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::engine::Database;
use crate::server::protocol;

type AppState = Arc<Database>;

#[derive(Deserialize)]
struct QueryRequest {
    q: String,
}

#[derive(Serialize)]
struct QueryResponse {
    results: Vec<SeriesResult>,
}

#[derive(Serialize)]
struct SeriesResult {
    series: Vec<SeriesData>,
}

#[derive(Serialize)]
struct SeriesData {
    name: String,
    columns: Vec<String>,
    values: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

#[derive(Serialize)]
struct StatusResponse {
    version: String,
    series_count: usize,
    points_in_memtable: usize,
    segment_count: usize,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn run_http_server(db: Arc<Database>, addr: &str) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/query", post(query_handler))
        .route("/lang", post(lang_handler))
        .route("/write", post(write_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr, "HTTP API server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn query_handler(
    State(db): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let result = db.query(&req.q).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    let mut values = Vec::new();
    for row in &result.rows {
        let mut row_values: Vec<serde_json::Value> = Vec::new();
        // "time" column first
        row_values.push(match row.timestamp {
            Some(ts) => serde_json::Value::Number(serde_json::Number::from(ts)),
            None => serde_json::Value::Null,
        });
        // Tag columns
        for col in &result.columns {
            if col == "time" {
                continue;
            }
            if let Some(tag_val) = row.tags.get(col) {
                row_values.push(serde_json::Value::String(tag_val.clone()));
                continue;
            }
            if let Some(&val) = row.values.get(col) {
                row_values.push(serde_json::json!(val));
            }
        }
        values.push(row_values);
    }

    let series_data = SeriesData {
        name: result.name,
        columns: result.columns,
        values,
    };

    Ok(Json(QueryResponse {
        results: vec![SeriesResult {
            series: vec![series_data],
        }],
    }))
}

#[derive(Serialize)]
struct LangResponse {
    result: String,
    #[serde(rename = "type")]
    result_type: String,
    elapsed_ns: u64,
}

async fn lang_handler(
    State(db): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<LangResponse>, (StatusCode, Json<ErrorResponse>)> {
    let start = std::time::Instant::now();
    let result = db.query_lang(&req.q).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    let elapsed = start.elapsed().as_nanos() as u64;

    Ok(Json(LangResponse {
        result_type: result.type_name().to_string(),
        result: format!("{result}"),
        elapsed_ns: elapsed,
    }))
}

async fn write_handler(
    State(db): State<AppState>,
    body: String,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let mut points = protocol::parse_lines(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    for point in &mut points {
        if point.timestamp == 0 {
            point.timestamp = now;
        }
    }

    db.write(points).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn status_handler(State(db): State<AppState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        series_count: db.series_count(),
        points_in_memtable: db.point_count(),
        segment_count: db.segment_count(),
    })
}
