use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use duckdb::arrow::array::Array as DuckArray;
use duckdb::arrow::ffi::to_ffi;

use errors::{Code, Error};

/// `duckdb`'s `arrow` (58.x) and the workspace's `arrow-array`/`arrow-schema`
/// (57.x) are different major versions, so `FFI_ArrowArray`/`FFI_ArrowSchema`
/// are distinct Rust types on each side of this boundary. Both are
/// `#[repr(C)]` structs implementing the same stable Arrow C Data Interface
/// spec with byte-for-byte identical field layout (verified against both
/// crates' source: same fields, same order, same primitive types — the only
/// difference is field visibility). Transmuting between them is exactly the
/// interop the C Data Interface exists for; each side's `Drop` impl just
/// invokes the `release` callback stored in the (identical) memory, so
/// ownership/cleanup stays correct across the transmute.
unsafe fn transmute_array(array: duckdb::arrow::ffi::FFI_ArrowArray) -> arrow_array::ffi::FFI_ArrowArray {
    unsafe { std::mem::transmute(array) }
}

unsafe fn transmute_schema(schema: duckdb::arrow::ffi::FFI_ArrowSchema) -> arrow_schema::ffi::FFI_ArrowSchema {
    unsafe { std::mem::transmute(schema) }
}

/// Converts one duckdb-side (arrow 58.x) `RecordBatch` into the workspace's
/// arrow-array 57.x `RecordBatch`, via the Arrow C Data Interface — the two
/// crates are distinct Rust types despite matching layout, so this is a
/// real conversion boundary, not a formality.
pub(crate) fn convert_batch(batch: &duckdb::arrow::record_batch::RecordBatch) -> Result<RecordBatch, Error> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let data = column.to_data();
        let (ffi_array, ffi_schema) = to_ffi(&data).map_err(|e| {
            Error::wrap_internal(e, Code::must_new("arrow_ffi_export_failed"), "failed to export duckdb arrow array via FFI")
        })?;
        let (ffi_array, ffi_schema) = unsafe { (transmute_array(ffi_array), transmute_schema(ffi_schema)) };
        let imported = unsafe { arrow_array::ffi::from_ffi(ffi_array, &ffi_schema) }.map_err(|e| {
            Error::wrap_internal(e, Code::must_new("arrow_ffi_import_failed"), "failed to import arrow array via FFI")
        })?;
        columns.push(arrow_array::make_array(imported));
    }

    let schema = convert_schema_ref(batch.schema().as_ref())?;
    RecordBatch::try_new(schema, columns)
        .map_err(|e| Error::wrap_internal(e, Code::must_new("arrow_batch_rebuild_failed"), "failed to rebuild record batch"))
}

pub(crate) fn convert_schema_ref(schema: &duckdb::arrow::datatypes::Schema) -> Result<Arc<Schema>, Error> {
    // Round-trip through the C Data Interface schema representation to get a
    // workspace-native `arrow_schema::Schema` from duckdb's arrow-58 one.
    let ffi_schema = duckdb::arrow::ffi::FFI_ArrowSchema::try_from(schema)
        .map_err(|e| Error::wrap_internal(e, Code::must_new("arrow_ffi_schema_export_failed"), "failed to export arrow schema via FFI"))?;
    let ffi_schema = unsafe { transmute_schema(ffi_schema) };
    let imported: arrow_schema::DataType = (&ffi_schema).try_into().map_err(|e: arrow_schema::ArrowError| {
        Error::wrap_internal(e, Code::must_new("arrow_ffi_schema_import_failed"), "failed to import arrow schema via FFI")
    })?;
    match imported {
        arrow_schema::DataType::Struct(fields) => Ok(Arc::new(Schema::new(fields))),
        other => Err(Error::new_internal(
            Code::must_new("arrow_ffi_schema_not_struct"),
            format!("expected struct datatype for schema import, got {other:?}"),
        )),
    }
}
