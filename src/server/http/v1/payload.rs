use std::collections::HashMap;
use std::sync::LazyLock;

use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use errors::{Code, Error};
use silo::ingest::{Column, ColumnType, TableSink, Value};
use silo::StreamId;

use crate::silo::AppState;

static CODE_UNKNOWN_COLUMN: LazyLock<Code> = LazyLock::new(|| Code::must_new("insert_unknown_column"));
static CODE_VALUE_TYPE_MISMATCH: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("insert_value_type_mismatch"));
static CODE_UNKNOWN_TABLE: LazyLock<Code> = LazyLock::new(|| Code::must_new("insert_unknown_table"));

#[derive(Deserialize)]
pub struct InsertIntoRequest {
    namespace: String,
    table: String,
    data: HashMap<String, JsonValue>,
}

#[derive(Serialize)]
pub struct Reponse {
    pub success: bool,
}

/// Coerce a JSON object into a row shaped like the table's *registered* schema:
/// values land in column order, and each one is parsed as the type `CREATE TABLE`
/// declared — not as whatever the JSON happened to look like. Absent keys are
/// `Null`; silo rejects that if the column is required.
///
/// A key the table does not have is an error rather than a silent drop: a typo'd
/// field name that quietly vanishes is worse than a rejected insert.
fn to_row(schema: &[Column], data: &HashMap<String, JsonValue>) -> Result<Vec<Value>, Error> {
    if let Some(unknown) = data.keys().find(|key| !schema.iter().any(|c| &&c.name == key)) {
        return Err(Error::new_invalid_input(
            CODE_UNKNOWN_COLUMN.clone(),
            format!("column '{unknown}' does not exist on this table"),
        ));
    }

    schema
        .iter()
        .map(|column| match data.get(&column.name) {
            None | Some(JsonValue::Null) => Ok(Value::Null),
            Some(value) => to_value(value, column),
        })
        .collect()
}

fn to_value(json: &JsonValue, column: &Column) -> Result<Value, Error> {
    let mismatch = || {
        Error::new_invalid_input(
            CODE_VALUE_TYPE_MISMATCH.clone(),
            format!("column '{}': cannot store {json} in a {:?} column", column.name, column.column_type),
        )
    };

    match (column.column_type, json) {
        (ColumnType::Bool, JsonValue::Bool(b)) => Ok(Value::Bool(*b)),
        (ColumnType::Int64, JsonValue::Number(n)) => n.as_i64().map(Value::Int64).ok_or_else(mismatch),
        (ColumnType::Float64, JsonValue::Number(n)) => n.as_f64().map(Value::Float64).ok_or_else(mismatch),
        (ColumnType::String, JsonValue::String(s)) => Ok(Value::String(s.clone())),
        _ => Err(mismatch()),
    }
}

/// ponytail: one request = one session = one Parquet file + one Iceberg snapshot.
/// Fine for the occasional insert, pathological as a bulk path — that is what
/// `COPY` is for. The upgrade, if this ever needs to take sustained traffic, is a
/// silo-owned per-stream buffer that flushes on size/time (ClickHouse's
/// `async_insert` shape), not a bigger batch here.
async fn ingest(state: &AppState, payload: &InsertIntoRequest) -> Result<(), Error> {
    let stream = StreamId::new([payload.namespace.clone()], payload.table.clone());

    let mut sink = state.sink.lock().await;
    let schema = sink.schema(&stream).await?.ok_or_else(|| {
        Error::new_not_found(
            CODE_UNKNOWN_TABLE.clone(),
            format!("table {stream} does not exist — create it first"),
        )
    })?;
    let row = to_row(&schema, &payload.data)?;

    let mut session = sink.begin_write(&stream).await?;
    session.push(row).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// id BIGINT, name TEXT
    fn schema() -> Vec<Column> {
        vec![Column::new("id", ColumnType::Int64, false), Column::new("name", ColumnType::String, false)]
    }

    fn data(pairs: &[(&str, JsonValue)]) -> HashMap<String, JsonValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn values_land_in_registered_column_order() {
        // Deliberately reversed relative to the schema — the row must not be.
        let row = to_row(&schema(), &data(&[("name", "a".into()), ("id", 1.into())])).unwrap();
        assert_eq!(row, vec![Value::Int64(1), Value::String("a".into())]);
    }

    #[test]
    fn a_missing_column_is_null_not_an_error() {
        let row = to_row(&schema(), &data(&[("id", 1.into())])).unwrap();
        assert_eq!(row, vec![Value::Int64(1), Value::Null]);
    }

    #[test]
    fn a_value_of_the_wrong_type_is_rejected() {
        // Under the old code this silently retyped the column to Utf8.
        assert!(to_row(&schema(), &data(&[("id", "not-a-number".into())])).is_err());
    }

    #[test]
    fn an_unknown_column_is_rejected_not_dropped() {
        assert!(to_row(&schema(), &data(&[("nope", 1.into())])).is_err());
    }
}
