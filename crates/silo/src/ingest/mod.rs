pub mod schema;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

use errors::{Code, Error};

use crate::destination::{Destination, DestinationSession};
use crate::StreamId;

static CODE_UNKNOWN_STREAM: LazyLock<Code> = LazyLock::new(|| Code::must_new("unknown_stream"));
static CODE_SCHEMA_EVOLUTION_UNSUPPORTED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("schema_evolution_unsupported"));

/// Layer 1: the entry point callers feed record batches to for a stream.
/// Named `TableSink` rather than "Writer" — iceberg-rust already exports a
/// public `IcebergWriter` trait, and reusing that name here would be
/// confusing to anyone importing both crates.
#[async_trait]
pub trait TableSink: Send + Sync {
    /// One-time setup for a stream: ensures the table exists (creating it
    /// with `desired_schema` if absent) in every configured destination, and
    /// records `desired_schema` as the schema subsequent write sessions are
    /// checked against.
    async fn setup(&mut self, stream: &StreamId, desired_schema: &IcebergSchema) -> Result<IcebergSchema, Error>;

    /// Begin a streaming ingest session for `stream`: feed it record
    /// batches as they arrive, then finish with `commit()` (fast-append
    /// everything written) or `abort()` (discard it, deleting any partial
    /// files — no destination is left with a half-written file or a
    /// dangling commit).
    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn IngestSession>, Error>;

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error>;
}

/// One streaming ingest session. Detects schema drift against the schema
/// recorded at `setup` time: the first `write` does a full diff; later
/// writes in the same session only re-diff if the batch's Arrow schema
/// object differs from the first one (iceberg-rust 0.9.1 supports no schema
/// evolution at all, so a later batch that passed the first check can never
/// legitimately introduce a new field — this guard exists to catch a
/// producer that starts feeding differently-shaped batches mid-session, not
/// for schema-evolution reasons).
#[async_trait]
pub trait IngestSession: Send {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), Error>;
    async fn commit(self: Box<Self>) -> Result<(), Error>;
    async fn abort(self: Box<Self>) -> Result<(), Error>;
}

pub struct IcebergTableSink {
    destination: Box<dyn Destination>,
    known_schemas: HashMap<StreamId, IcebergSchema>,
}

impl IcebergTableSink {
    pub fn new(destination: Box<dyn Destination>) -> Self {
        Self { destination, known_schemas: HashMap::new() }
    }
}

#[async_trait]
impl TableSink for IcebergTableSink {
    async fn setup(&mut self, stream: &StreamId, desired_schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        self.destination.ensure_table(stream, desired_schema).await?;
        self.known_schemas.insert(stream.clone(), desired_schema.clone());
        Ok(desired_schema.clone())
    }

    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn IngestSession>, Error> {
        let known_schema = self
            .known_schemas
            .get(stream)
            .ok_or_else(|| Error::new_not_found(CODE_UNKNOWN_STREAM.clone(), format!("unknown stream {stream:?}")))?
            .clone();
        let inner = self.destination.begin_write(stream).await?;
        Ok(Box::new(IcebergIngestSession {
            stream: stream.clone(),
            known_schema,
            first_batch_schema: None,
            inner,
        }))
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.known_schemas.remove(stream);
        self.destination.close(stream).await
    }
}

struct IcebergIngestSession {
    stream: StreamId,
    known_schema: IcebergSchema,
    first_batch_schema: Option<Arc<ArrowSchema>>,
    inner: Box<dyn DestinationSession>,
}

impl IcebergIngestSession {
    /// iceberg-rust 0.9.1 exposes no public way to evolve an existing
    /// table's schema (see `IcebergBackend::evolve_schema`), so any added
    /// field is unwritable, not just unexpected.
    fn check_drift(&self, batch: &RecordBatch) -> Result<(), Error> {
        let incoming = schema::to_iceberg_schema(batch.schema().as_ref())?;
        let added = schema::added_fields(&self.known_schema, &incoming);
        if added.is_empty() {
            return Ok(());
        }
        Err(Error::new_unsupported(
            CODE_SCHEMA_EVOLUTION_UNSUPPORTED.clone(),
            format!(
                "schema evolution for stream {:?} is not supported by iceberg-rust 0.9.1 \
                 (no public Transaction::update_schema); new/changed fields: {added:?}",
                self.stream
            ),
        ))
    }
}

#[async_trait]
impl IngestSession for IcebergIngestSession {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), Error> {
        match &self.first_batch_schema {
            None => {
                self.check_drift(&batch)?;
                self.first_batch_schema = Some(batch.schema());
            }
            Some(first) if !Arc::ptr_eq(first, &batch.schema()) => self.check_drift(&batch)?,
            Some(_) => {}
        }
        self.inner.write(&batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        self.inner.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<(), Error> {
        self.inner.abort().await
    }
}
