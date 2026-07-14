use std::sync::LazyLock;

use arrow_cast::display::array_value_to_string;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use errors::{Code, Error};

use crate::querier::intercepter::{Intercepter, Outcome};
use crate::querier::QueryResult;
use crate::silo::AppState;

static CODE_RESULT_ENCODE_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("query_result_encode_failed"));
static CODE_COPY_OVER_HTTP: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_over_http"));

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
pub struct QueryResponseJson {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

/// Renders what the [`Intercepter`] did as a JSON body.
///
/// `COPY ... FROM STDIN` is a streaming protocol mode with no request/response
/// equivalent, so the [`crate::querier::intercepter::CopyInFlight`] is dropped rather
/// than stored — no ingest session is open yet, so nothing leaks.
fn to_response(outcome: Outcome) -> Result<QueryResponseJson, Error> {
    match outcome {
        Outcome::Created => Ok(QueryResponseJson {
            columns: vec!["status".to_string()],
            rows: vec![vec![Value::String("CREATE TABLE".to_string())]],
        }),
        Outcome::Rows(result) => to_json(result),
        Outcome::CopyIn(_) => Err(Error::new_unsupported(
            CODE_COPY_OVER_HTTP.clone(),
            "COPY ... FROM STDIN is only available over the Postgres wire protocol",
        )),
    }
}

fn to_json(result: QueryResult) -> Result<QueryResponseJson, Error> {
    let columns: Vec<String> = result.schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(columns.len());
            for column in batch.columns() {
                if column.is_null(row_idx) {
                    row.push(Value::Null);
                } else {
                    let text = array_value_to_string(column.as_ref(), row_idx).map_err(|e| {
                        Error::wrap_internal(e, CODE_RESULT_ENCODE_FAILED.clone(), "failed to encode query result value")
                    })?;
                    row.push(Value::String(text));
                }
            }
            rows.push(row);
        }
    }
    Ok(QueryResponseJson { columns, rows })
}

pub async fn run_query(State(state): State<AppState>, Json(payload): Json<QueryRequest>) -> impl IntoResponse {
    let intercepter = Intercepter::new(&state);
    match intercepter.visit(&payload.sql).await.and_then(to_response) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => (StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), Json(e.as_json())).into_response(),
    }
}
