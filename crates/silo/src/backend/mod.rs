mod common;
pub mod filesystem;
pub mod s3;

use async_trait::async_trait;
use arrow_array::RecordBatch;
use iceberg::spec::Schema as IcebergSchema;

use errors::Error;

use crate::StreamId;

/// Layer 3: one concrete storage-backend implementation. Owns the iceberg
/// `Catalog` + `Storage`/`FileIO` for its backend, the loaded `Table` per
/// stream, and the actual Parquet-write + transaction-commit calls.
#[async_trait]
pub trait DestinationWriter: Send + Sync {
    fn name(&self) -> &str;

    /// Load or create the table for `stream`, returning its current schema.
    async fn ensure_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error>;

    /// Write one RecordBatch as a Parquet data file and append it via a
    /// fast-append transaction commit against this destination's catalog.
    async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), Error>;

    /// Returns an unsupported-typed [`Error`] — not implementable against
    /// iceberg-rust 0.9.1 today (see `IcebergBackend::evolve_schema`).
    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<IcebergSchema, Error>;

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error>;

    /// Cheap connectivity/permissions probe.
    async fn check(&self) -> Result<(), Error>;
}
