use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::engine::Database;
use crate::lang::value::Value;
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
    measurements: Vec<String>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
struct WsClientMessage {
    action: String,
    id: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    interval_ms: Option<u64>,
}

#[derive(Serialize)]
struct WsDataMessage {
    id: String,
    #[serde(flatten)]
    data: serde_json::Value,
    timestamp: i64,
}

pub async fn run_http_server(db: Arc<Database>, addr: &str) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/query", post(query_handler))
        .route("/lang", post(lang_handler))
        .route("/write", post(write_handler))
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .route("/measurements", get(measurements_handler))
        .route("/fields", get(fields_handler))
        .route("/ws", get(ws_handler))
        .layer(cors)
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
        row_values.push(match row.timestamp {
            Some(ts) => serde_json::Value::Number(serde_json::Number::from(ts)),
            None => serde_json::Value::Null,
        });
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

/// Convert a PulseLang Value to a structured JSON response for the UI.
fn value_to_json(value: &Value, elapsed_ns: u64) -> serde_json::Value {
    match value {
        Value::Int(v) => serde_json::json!({
            "type": "int",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::UInt(v) => serde_json::json!({
            "type": "uint",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Float(v) => serde_json::json!({
            "type": "float",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Bool(v) => serde_json::json!({
            "type": "bool",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Str(v) => serde_json::json!({
            "type": "str",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Symbol(v) => serde_json::json!({
            "type": "sym",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Timestamp(v) => serde_json::json!({
            "type": "ts",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Duration(v) => serde_json::json!({
            "type": "dur",
            "value": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::Null => serde_json::json!({
            "type": "null",
            "value": null,
            "elapsed_ns": elapsed_ns,
        }),
        Value::IntVec(v) => serde_json::json!({
            "type": "int[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::FloatVec(v) => serde_json::json!({
            "type": "float[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::BoolVec(v) => serde_json::json!({
            "type": "bool[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::SymVec(v) => serde_json::json!({
            "type": "sym[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::StrVec(v) => serde_json::json!({
            "type": "str[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::TimestampVec(v) => serde_json::json!({
            "type": "ts[]",
            "values": v,
            "elapsed_ns": elapsed_ns,
        }),
        Value::List(items) => {
            let vals: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v, 0)).collect();
            serde_json::json!({
                "type": "list",
                "values": vals,
                "elapsed_ns": elapsed_ns,
            })
        }
        Value::Dict(d) => {
            let mut entries = serde_json::Map::new();
            for (k, v) in d {
                entries.insert(k.clone(), value_to_json(v, 0));
            }
            serde_json::json!({
                "type": "dict",
                "entries": entries,
                "elapsed_ns": elapsed_ns,
            })
        }
        Value::Table(table) => {
            let mut data = serde_json::Map::new();
            for (col_name, col_val) in &table.data {
                data.insert(col_name.clone(), column_to_json(col_val));
            }
            let row_count = table.data.values().next().map_or(0, |v| v.count());
            serde_json::json!({
                "type": "table",
                "columns": table.columns,
                "data": data,
                "row_count": row_count,
                "elapsed_ns": elapsed_ns,
            })
        }
        Value::Lambda { params, .. } => serde_json::json!({
            "type": "fn",
            "params": params,
            "elapsed_ns": elapsed_ns,
        }),
        Value::BuiltinFn(name) => serde_json::json!({
            "type": "fn",
            "name": name,
            "elapsed_ns": elapsed_ns,
        }),
    }
}

/// Convert a column Value to a JSON array for the table response.
fn column_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::IntVec(v) => serde_json::json!(v),
        Value::FloatVec(v) => serde_json::json!(v),
        Value::BoolVec(v) => serde_json::json!(v),
        Value::SymVec(v) => serde_json::json!(v),
        Value::StrVec(v) => serde_json::json!(v),
        Value::TimestampVec(v) => serde_json::json!(v),
        _ => serde_json::json!(format!("{value}")),
    }
}

async fn lang_handler(
    State(db): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
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

    Ok(Json(value_to_json(&result, elapsed)))
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
        measurements: db.measurement_names(),
    })
}

#[derive(Serialize)]
struct MeasurementsResponse {
    measurements: Vec<String>,
}

async fn measurements_handler(State(db): State<AppState>) -> Json<MeasurementsResponse> {
    Json(MeasurementsResponse {
        measurements: db.measurement_names(),
    })
}

#[derive(Deserialize)]
struct FieldsQuery {
    measurement: String,
}

#[derive(Serialize)]
struct FieldsResponse {
    measurement: String,
    fields: Vec<String>,
}

async fn fields_handler(
    State(db): State<AppState>,
    Query(params): Query<FieldsQuery>,
) -> Json<FieldsResponse> {
    Json(FieldsResponse {
        measurement: params.measurement.clone(),
        fields: db.field_names(&params.measurement),
    })
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(db): State<AppState>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, db))
}

async fn handle_ws(mut socket: WebSocket, db: Arc<Database>) {
    let (tx, mut rx) = mpsc::channel::<String>(64);

    let subscriptions: Arc<parking_lot::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));

    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let parsed: Result<WsClientMessage, _> = serde_json::from_str(&text);
                        match parsed {
                            Ok(client_msg) => {
                                handle_ws_message(client_msg, &db, &tx, &subscriptions).await;
                            }
                            Err(e) => {
                                let err = serde_json::json!({"error": e.to_string()});
                                let _ = socket.send(Message::Text(err.to_string())).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    let subs = subscriptions.lock();
    for (_, handle) in subs.iter() {
        handle.abort();
    }
}

async fn handle_ws_message(
    msg: WsClientMessage,
    db: &Arc<Database>,
    tx: &mpsc::Sender<String>,
    subscriptions: &Arc<parking_lot::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
) {
    match msg.action.as_str() {
        "subscribe" => {
            let query = match msg.query {
                Some(q) => q,
                None => return,
            };
            let interval_ms = msg.interval_ms.unwrap_or(1000).max(100);
            let id = msg.id.clone();

            {
                let mut subs = subscriptions.lock();
                if let Some(handle) = subs.remove(&id) {
                    handle.abort();
                }
            }

            let db = Arc::clone(db);
            let tx = tx.clone();
            let sub_id = id.clone();

            let handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    tokio::time::Duration::from_millis(interval_ms),
                );
                let mut last_json: Option<serde_json::Value> = None;

                loop {
                    interval.tick().await;

                    let db_ref = Arc::clone(&db);
                    let query_clone = query.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        db_ref.query_lang(&query_clone)
                    }).await;

                    let value = match result {
                        Ok(Ok(v)) => v,
                        Ok(Err(e)) => {
                            let err_msg = serde_json::json!({
                                "id": sub_id,
                                "error": e.to_string(),
                            });
                            if tx.send(err_msg.to_string()).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        Err(_) => break,
                    };

                    let data = value_to_json(&value, 0);

                    if last_json.as_ref() == Some(&data) {
                        continue;
                    }
                    last_json = Some(data.clone());

                    let push = WsDataMessage {
                        id: sub_id.clone(),
                        data,
                        timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    };

                    match serde_json::to_string(&push) {
                        Ok(json) => {
                            if tx.send(json).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            subscriptions.lock().insert(id, handle);
        }
        "unsubscribe" => {
            let mut subs = subscriptions.lock();
            if let Some(handle) = subs.remove(&msg.id) {
                handle.abort();
            }
        }
        _ => {}
    }
}
