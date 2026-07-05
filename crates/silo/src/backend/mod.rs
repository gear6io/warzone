mod common;
mod parquet_writer;
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

    /// Begin a streaming write session for `stream`: records fed to the
    /// returned [`WriteSession`] land in one open Parquet file until either
    /// `commit` (fast-append it via a transaction commit) or `abort`
    /// (discard it, deleting any partial file) is called.
    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn WriteSession>, Error>;

    /// Returns an unsupported-typed [`Error`] — not implementable against
    /// iceberg-rust 0.9.1 today (see `IcebergBackend::evolve_schema`).
    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<IcebergSchema, Error>;

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error>;

    /// Cheap connectivity/permissions probe.
    async fn check(&self) -> Result<(), Error>;
}

/// One streaming write session against a single destination backend: zero
/// or more record batches, then exactly one of `commit`/`abort`. Neither
/// consuming method may be called twice, and no other method may be called
/// after either.
#[async_trait]
pub trait WriteSession: Send {
    /// Write one record batch to the session's open Parquet file.
    async fn write(&mut self, batch: RecordBatch) -> Result<(), Error>;

    /// Finalize the session's file and fast-append it via a transaction
    /// commit against this destination's catalog.
    async fn commit(self: Box<Self>) -> Result<(), Error>;

    /// Cancel the session: delete its partial file, if any was ever
    /// opened, and never touch the catalog.
    async fn abort(self: Box<Self>) -> Result<(), Error>;
}
