use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use errors::{Code, Error};
use silo::ingest::schema::to_iceberg_schema;
use silo::ingest::TableSink;
use silo::StreamId;

use crate::silo::AppState;

#[derive(Deserialize)]
pub struct InsertIntoRequest {
    namespace: String,
    table: String,
    data: HashMap<String, Value>,
}

#[derive(Serialize)]
pub struct Reponse {
    pub success: bool,
}

fn json_to_record_batch(data: &HashMap<String, Value>) -> Result<RecordBatch, Error> {
    let mut fields = Vec::with_capacity(data.len());
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(data.len());

    for (name, value) in data {
        let (data_type, column): (DataType, Arc<dyn Array>) = match value {
            Value::String(s) => (DataType::Utf8, Arc::new(StringArray::from(vec![s.clone()]))),
            Value::Number(n) if n.is_i64() || n.is_u64() => (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![n.as_i64().unwrap()])),
            ),
            Value::Number(n) => (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![n.as_f64().unwrap()])),
            ),
            Value::Bool(b) => (DataType::Boolean, Arc::new(BooleanArray::from(vec![*b]))),
            Value::Null | Value::Array(_) | Value::Object(_) => {
                return Err(Error::new_invalid_input(
                    Code::must_new("unsupported_field_value"),
                    format!("field '{name}': null/array/object values are not supported"),
                ));
            }
        };
        fields.push(Field::new(name, data_type, false));
        columns.push(column);
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| {
        Error::wrap_invalid_input(
            e,
            Code::must_new("record_batch_build_failed"),
            "failed to build record batch from payload",
        )
    })
}

async fn ingest(state: &AppState, payload: &InsertIntoRequest) -> Result<(), Error> {
    let batch = json_to_record_batch(&payload.data)?;
    let stream = StreamId::new([payload.namespace.clone()], payload.table.clone());
    let iceberg_schema = to_iceberg_schema(batch.schema().as_ref())?;

    let mut sink = state.sink.lock().await;
    sink.setup(&stream, &iceberg_schema).await?;
    let mut session = sink.begin_write(&stream).await?;
    session.write(batch).await?;
    session.commit().await?;
    Ok(())
}

pub async fn accept_payload(
    State(state): State<AppState>,
    Json(payload): Json<InsertIntoRequest>,
) -> impl IntoResponse {
    match ingest(&state, &payload).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(Reponse { success: true })).into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(e.as_json()),
        )
            .into_response(),
    }
}
