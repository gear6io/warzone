use std::sync::{Arc, LazyLock};

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};

use errors::{Code, Error};

use crate::destination::{Destination, DestinationSession};
use crate::{wrap_iceberg, StreamId};

static CODE_UNKNOWN_STREAM: LazyLock<Code> = LazyLock::new(|| Code::must_new("unknown_stream"));
static CODE_ROW_ARITY_MISMATCH: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("row_arity_mismatch"));
static CODE_VALUE_TYPE_MISMATCH: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("value_type_mismatch"));
static CODE_UNSUPPORTED_COLUMN_TYPE: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("unsupported_column_type"));
static CODE_SCHEMA_BUILD_FAILED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("schema_build_failed"));
static CODE_BATCH_BUILD_FAILED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("batch_build_failed"));

/// The type of one column, in silo's own vocabulary.
///
/// Neither Arrow nor Iceberg appears in silo's public API. Both are internal
/// implementation details of *how* rows get to storage; a caller registering a
/// table or pushing a record should not have to learn either one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    /// 64-bit integer.
    Int64,
    /// 64-bit float.
    Float64,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    /// `NOT NULL`. A required column rejects a `Value::Null`.
    pub required: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, column_type: ColumnType, required: bool) -> Self {
        Self {
            name: name.into(),
            column_type,
            required,
        }
    }
}

/// One scalar on its way into a table — the entire value vocabulary callers
/// speak. Wire handlers parse their own format (CSV text, JSON, ...) into
/// `Value`s against the registered [`ColumnType`]s and hand silo rows.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
}

/// One record, positionally aligned with the registered columns.
pub type Row = Vec<Value>;

/// Layer 1: register a table's schema once, then push records at it.
#[async_trait]
pub trait TableSink: Send + Sync {
    /// Create `stream`'s table with `columns`. Errors (`AlreadyExists`) if it is
    /// already registered. This is the *only* way a table comes into being —
    /// ingest never creates one implicitly, so a schema is never guessed from
    /// whatever row happened to arrive first.
    async fn register(&mut self, stream: &StreamId, columns: &[Column]) -> Result<(), Error>;

    /// `stream`'s authoritative columns as the catalog holds them. `None` if it
    /// is not registered. Callers coerce their wire values into *these* types.
    async fn schema(&mut self, stream: &StreamId) -> Result<Option<Vec<Column>>, Error>;

    /// Open an ingest session against a registered stream. `NotFound` if it is
    /// not registered.
    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn IngestSession>, Error>;

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error>;
}

/// One ingest session: push rows, then exactly one of `commit`/`abort`.
#[async_trait]
pub trait IngestSession: Send {
    /// Push one record.
    ///
    /// Rows accumulate into Arrow column builders here and flush as a
    /// `RecordBatch` every `batch_size` rows, plus once more at `commit`.
    ///
    /// **This is the one place batching exists, and it exists because Parquet
    /// requires it.** Parquet's physical layout is row groups → pages, so a row
    /// can never reach storage on its own — something, somewhere, must gather
    /// rows into a columnar block first. A row-at-a-time API over an internally
    /// batching writer is the normal shape for exactly this reason. Putting that
    /// here (rather than at each caller, as it used to be) is what lets every
    /// caller stay row-shaped, and keeps the memory question — "how many rows am
    /// I holding?" — to a single answer: at most `batch_size`.
    async fn push(&mut self, row: Row) -> Result<(), Error>;

    /// Flush what's buffered, then fast-append the session's file as one snapshot.
    async fn commit(self: Box<Self>) -> Result<(), Error>;

    /// Discard the session: delete any partial file, never touch the catalog.
    async fn abort(self: Box<Self>) -> Result<(), Error>;
}

pub struct IcebergTableSink {
    destination: Box<dyn Destination>,
    batch_size: usize,
}

impl IcebergTableSink {
    pub fn new(destination: Box<dyn Destination>, batch_size: usize) -> Self {
        Self {
            destination,
            batch_size,
        }
    }
}

#[async_trait]
impl TableSink for IcebergTableSink {
    async fn register(&mut self, stream: &StreamId, columns: &[Column]) -> Result<(), Error> {
        let schema = to_iceberg(columns)?;
        self.destination
            .create_table(stream, &schema)
            .await
            .map(|_| ())
    }

    async fn schema(&mut self, stream: &StreamId) -> Result<Option<Vec<Column>>, Error> {
        match self.destination.load_table(stream).await? {
            Some(schema) => from_iceberg(&schema).map(Some),
            None => Ok(None),
        }
    }

    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn IngestSession>, Error> {
        // Read the schema back from the catalog rather than a local cache: it is
        // the authority, and one load per session (not per row) is free.
        let columns = self.schema(stream).await?.ok_or_else(|| {
            Error::new_not_found(
                CODE_UNKNOWN_STREAM.clone(),
                format!("table {stream} is not registered — create it first"),
            )
        })?;

        let arrow_schema = Arc::new(ArrowSchema::new(
            columns
                .iter()
                .map(|c| Field::new(&c.name, arrow_type(c.column_type), !c.required))
                .collect::<Vec<_>>(),
        ));
        let builders = columns.iter().map(ColumnBuilder::new).collect();

        let inner = self.destination.begin_write(stream).await?;
        Ok(Box::new(IcebergIngestSession {
            arrow_schema,
            builders,
            buffered: 0,
            batch_size: self.batch_size,
            inner,
        }))
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.destination.close(stream).await
    }
}

fn arrow_type(column_type: ColumnType) -> DataType {
    match column_type {
        ColumnType::Bool => DataType::Boolean,
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float64 => DataType::Float64,
        ColumnType::String => DataType::Utf8,
    }
}

/// silo's columns -> an Iceberg schema, with field ids assigned in order.
fn to_iceberg(columns: &[Column]) -> Result<IcebergSchema, Error> {
    let fields: Vec<Arc<NestedField>> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let id = index as i32 + 1;
            let field_type = IcebergType::Primitive(match column.column_type {
                ColumnType::Bool => PrimitiveType::Boolean,
                ColumnType::Int64 => PrimitiveType::Long,
                ColumnType::Float64 => PrimitiveType::Double,
                ColumnType::String => PrimitiveType::String,
            });
            Arc::new(if column.required {
                NestedField::required(id, &column.name, field_type)
            } else {
                NestedField::optional(id, &column.name, field_type)
            })
        })
        .collect();

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| {
            wrap_iceberg(
                e,
                CODE_SCHEMA_BUILD_FAILED.clone(),
                "failed to build Iceberg schema",
            )
        })
}

/// An Iceberg schema -> silo's columns. A column whose type silo cannot ingest
/// is an error rather than a silent skip: a table we can only half-write is not
/// a table we should accept rows for.
fn from_iceberg(schema: &IcebergSchema) -> Result<Vec<Column>, Error> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            let unsupported = || {
                Error::new_unsupported(
                    CODE_UNSUPPORTED_COLUMN_TYPE.clone(),
                    format!(
                        "column '{}': type {} is not supported for ingest yet",
                        field.name, field.field_type
                    ),
                )
            };
            let IcebergType::Primitive(primitive) = field.field_type.as_ref() else {
                return Err(unsupported());
            };
            let column_type = match primitive {
                PrimitiveType::Boolean => ColumnType::Bool,
                PrimitiveType::Long => ColumnType::Int64,
                PrimitiveType::Double => ColumnType::Float64,
                PrimitiveType::String => ColumnType::String,
                _ => return Err(unsupported()),
            };
            Ok(Column {
                name: field.name.clone(),
                column_type,
                required: field.required,
            })
        })
        .collect()
}

struct IcebergIngestSession {
    arrow_schema: Arc<ArrowSchema>,
    builders: Vec<ColumnBuilder>,
    buffered: usize,
    batch_size: usize,
    inner: Box<dyn DestinationSession>,
}

impl IcebergIngestSession {
    async fn flush(&mut self) -> Result<(), Error> {
        if self.buffered == 0 {
            return Ok(());
        }
        // `finish` drains each builder and leaves it empty, ready for the next batch.
        let columns: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        self.buffered = 0;

        let batch = RecordBatch::try_new(self.arrow_schema.clone(), columns).map_err(|e| {
            Error::wrap_invalid_input(
                e,
                CODE_BATCH_BUILD_FAILED.clone(),
                "failed to build record batch from rows",
            )
        })?;
        self.inner.write(&batch).await
    }
}

#[async_trait]
impl IngestSession for IcebergIngestSession {
    async fn push(&mut self, row: Row) -> Result<(), Error> {
        if row.len() != self.builders.len() {
            return Err(Error::new_invalid_input(
                CODE_ROW_ARITY_MISMATCH.clone(),
                format!(
                    "row has {} values but the table has {} columns",
                    row.len(),
                    self.builders.len()
                ),
            ));
        }
        for (builder, value) in self.builders.iter_mut().zip(row) {
            builder.push(value)?;
        }
        self.buffered += 1;

        if self.buffered >= self.batch_size {
            self.flush().await?;
        }
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.flush().await?;
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<(), Error> {
        self.inner.abort().await
    }
}

/// One column's Arrow builder, chosen from its registered [`ColumnType`]. Rows
/// are appended into these; `finish` turns them into a `RecordBatch`'s columns.
enum Builder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
}

struct ColumnBuilder {
    name: String,
    builder: Builder,
}

impl ColumnBuilder {
    fn new(column: &Column) -> Self {
        let builder = match column.column_type {
            ColumnType::Bool => Builder::Bool(BooleanBuilder::new()),
            ColumnType::Int64 => Builder::Int64(Int64Builder::new()),
            ColumnType::Float64 => Builder::Float64(Float64Builder::new()),
            ColumnType::String => Builder::String(StringBuilder::new()),
        };
        Self {
            name: column.name.clone(),
            builder,
        }
    }

    fn push(&mut self, value: Value) -> Result<(), Error> {
        let mismatch = || {
            Error::new_invalid_input(
                CODE_VALUE_TYPE_MISMATCH.clone(),
                format!("column '{}': cannot store {value:?}", self.name),
            )
        };

        match (&mut self.builder, &value) {
            (Builder::Bool(b), Value::Null) => b.append_null(),
            (Builder::Int64(b), Value::Null) => b.append_null(),
            (Builder::Float64(b), Value::Null) => b.append_null(),
            (Builder::String(b), Value::Null) => b.append_null(),

            (Builder::Bool(b), Value::Bool(v)) => b.append_value(*v),
            (Builder::Int64(b), Value::Int64(v)) => b.append_value(*v),
            (Builder::Float64(b), Value::Float64(v)) => b.append_value(*v),
            (Builder::String(b), Value::String(v)) => b.append_value(v),

            _ => return Err(mismatch()),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match &mut self.builder {
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::Int64(b) => Arc::new(b.finish()),
            Builder::Float64(b) => Arc::new(b.finish()),
            Builder::String(b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    fn column(column_type: ColumnType) -> Column {
        Column::new("c", column_type, false)
    }

    #[test]
    fn a_value_that_does_not_match_the_column_type_is_rejected() {
        // Under the old first-row inference this silently retyped the column.
        let mut builder = ColumnBuilder::new(&column(ColumnType::Int64));
        assert!(builder.push(Value::String("not-a-number".into())).is_err());
    }

    #[test]
    fn a_text_column_keeps_a_numeric_looking_string_intact() {
        // The leading-zero bug: inference turned "01234" into the integer 1234.
        // With a registered TEXT column the string survives.
        let mut builder = ColumnBuilder::new(&column(ColumnType::String));
        builder.push(Value::String("01234".into())).unwrap();
        let array = builder.finish();
        let strings = array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), "01234");
    }

    #[test]
    fn nulls_are_accepted_by_every_column_type() {
        for column_type in [
            ColumnType::Bool,
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::String,
        ] {
            let mut builder = ColumnBuilder::new(&column(column_type));
            builder.push(Value::Null).unwrap();
            assert!(builder.finish().is_null(0));
        }
    }

    /// Registration and lookup must agree, or a COPY would coerce rows against a
    /// different shape than the table actually has.
    #[test]
    fn columns_round_trip_through_the_iceberg_schema() {
        let columns = vec![
            Column::new("id", ColumnType::Int64, true),
            Column::new("name", ColumnType::String, false),
            Column::new("score", ColumnType::Float64, false),
            Column::new("ok", ColumnType::Bool, false),
        ];
        assert_eq!(
            from_iceberg(&to_iceberg(&columns).unwrap()).unwrap(),
            columns
        );
    }
}
