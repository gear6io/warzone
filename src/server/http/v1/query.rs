use arrow_cast::display::array_value_to_string;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use errors::{Code, Error};
use crate::querier::QueryResult;

use crate::silo::AppState;

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
pub struct QueryResponseJson {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
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
                        Error::wrap_internal(e, Code::must_new("query_result_encode_failed"), "failed to encode query result value")
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
    match state.querier.query(&payload.sql).await.and_then(to_json) {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => (StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), Json(e.as_json())).into_response(),
    }
}
