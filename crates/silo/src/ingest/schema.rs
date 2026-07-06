use std::sync::LazyLock;

use arrow_schema::Schema as ArrowSchema;
use errors::{Code, Error};
use iceberg::spec::Schema as IcebergSchema;

use crate::wrap_iceberg;

static CODE_INCOMING_SCHEMA_CONVERSION_FAILED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("incoming_schema_conversion_failed"));

/// Converts an Arrow schema (as carried by an incoming `RecordBatch`) into
/// an Iceberg schema, via iceberg-rust's own conversion — field-ID/type
/// mapping is not reimplemented here.
///
/// Uses `arrow_schema_to_schema_auto_assign_ids` rather than
/// `arrow_schema_to_schema`: the latter requires every Arrow field to
/// already carry an Iceberg field-id in its metadata, which source data
/// (e.g. from a DB connector) won't have. Auto-assignment is exactly what
/// iceberg-rust recommends for "schemas that don't originate from Iceberg
/// tables".
pub fn to_iceberg_schema(schema: &ArrowSchema) -> Result<IcebergSchema, Error> {
    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(schema).map_err(|e| {
        wrap_iceberg(e, CODE_INCOMING_SCHEMA_CONVERSION_FAILED.clone(), "failed to convert Arrow schema to Iceberg")
    })
}

/// Names present in `incoming` but missing from `current`. Empty means no
/// drift: the incoming batch can be written as-is.
pub fn added_fields(current: &IcebergSchema, incoming: &IcebergSchema) -> Vec<String> {
    incoming
        .as_struct()
        .fields()
        .iter()
        .filter(|f| current.field_by_name(&f.name).is_none())
        .map(|f| f.name.clone())
        .collect()
}
