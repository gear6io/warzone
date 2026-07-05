pub mod schema;

use std::collections::HashMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

use crate::destination::Destination;
use crate::{SinkError, StreamId};

/// Layer 1: the entry point callers feed record batches to for a stream.
/// Named `TableSink` rather than "Writer" — iceberg-rust already exports a
/// public `IcebergWriter` trait, and reusing that name here would be
/// confusing to anyone importing both crates.
#[async_trait]
pub trait TableSink: Send + Sync {
    /// One-time setup for a stream: ensures the table exists (creating it
    /// with `desired_schema` if absent) in every configured destination, and
    /// records `desired_schema` as the schema subsequent `write` calls are
    /// checked against.
    async fn setup(&mut self, stream: &StreamId, desired_schema: &IcebergSchema) -> Result<IcebergSchema, SinkError>;

    /// Ingest one batch for `stream`. Detects schema drift against the
    /// schema recorded at `setup` time; if the batch introduces new/changed
    /// fields, returns [`SinkError::SchemaEvolutionUnsupported`] rather than
    /// writing a batch the table can't actually represent.
    async fn write(&mut self, stream: &StreamId, batch: RecordBatch) -> Result<(), SinkError>;

    async fn close(&mut self, stream: &StreamId) -> Result<(), SinkError>;
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
    async fn setup(&mut self, stream: &StreamId, desired_schema: &IcebergSchema) -> Result<IcebergSchema, SinkError> {
        self.destination.ensure_table(stream, desired_schema).await?;
        self.known_schemas.insert(stream.clone(), desired_schema.clone());
        Ok(desired_schema.clone())
    }

    async fn write(&mut self, stream: &StreamId, batch: RecordBatch) -> Result<(), SinkError> {
        let known = self
            .known_schemas
            .get(stream)
            .ok_or_else(|| SinkError::UnknownStream(stream.clone()))?;

        let incoming = schema::to_iceberg_schema(batch.schema().as_ref())?;
        let added = schema::added_fields(known, &incoming);
        if !added.is_empty() {
            return Err(SinkError::SchemaEvolutionUnsupported { stream: stream.clone(), fields: added });
        }

        self.destination.write(stream, &batch).await
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), SinkError> {
        self.known_schemas.remove(stream);
        self.destination.close(stream).await
    }
}
