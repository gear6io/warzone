use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::Schema as IcebergSchema;

use crate::SinkError;

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
pub fn to_iceberg_schema(schema: &ArrowSchema) -> Result<IcebergSchema, SinkError> {
    Ok(iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(schema)?)
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
