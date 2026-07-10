//! Converts a [`crate::querier::QueryResult`] (Arrow batches) into a pgwire
//! `QueryResponse`. Every column is declared `TEXT` and encoded via
//! `arrow_cast`'s value-to-string formatter — text-format Postgres wire
//! rows work correctly regardless of the declared OID (psql just prints
//! what it gets), so this sidesteps hand-writing a per-`DataType` binary
//! encoder for a first pass.
use std::sync::Arc;

use arrow_cast::display::array_value_to_string;
use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

use crate::querier::QueryResult;

pub(crate) fn record_batches_to_query_response(result: QueryResult) -> PgWireResult<QueryResponse> {
    let fields: Vec<FieldInfo> = result
        .schema
        .fields()
        .iter()
        .map(|f| FieldInfo::new(f.name().clone(), None, None, Type::TEXT, FieldFormat::Text))
        .collect();
    let schema = Arc::new(fields);

    let mut rows = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for column in batch.columns() {
                let value = if column.is_null(row_idx) {
                    None
                } else {
                    Some(
                        array_value_to_string(column.as_ref(), row_idx)
                            .map_err(|e| PgWireError::ApiError(Box::new(e)))?,
                    )
                };
                encoder.encode_field(&value)?;
            }
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(QueryResponse::new(schema, stream::iter(rows)))
}
