pub mod multi;
pub mod single;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

pub use multi::MultiDestination;
pub use single::SingleDestination;

use errors::Error;

use crate::StreamId;

/// Layer 2: fans a stream of record batches out to one ([`SingleDestination`])
/// or several ([`MultiDestination`]) independently-configured backends.
/// Callers (Layer 1, [`crate::ingest::TableSink`]) don't need to know which.
#[async_trait]
pub trait Destination: Send + Sync {
    /// Load an already-registered table's authoritative schema. `None` if it is
    /// not registered anywhere.
    async fn load_table(&mut self, stream: &StreamId) -> Result<Option<IcebergSchema>, Error>;

    /// Create the table with `schema` in every backend.
    async fn create_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error>;

    /// Begin a streaming write session, fanned out to every underlying
    /// backend. The returned [`DestinationSession`] is itself a fan-out:
    /// `write` sends the same batch to every backend, `commit`/`abort`
    /// finalize or cancel every backend's session together.
    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn DestinationSession>, Error>;
    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<(), Error>;
    async fn close(&mut self, stream: &StreamId) -> Result<(), Error>;
}

/// One streaming write session, possibly fanned out across several
/// destinations. Mirrors [`crate::backend::WriteSession`] one layer up.
#[async_trait]
pub trait DestinationSession: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), Error>;
    async fn commit(self: Box<Self>) -> Result<(), Error>;
    async fn abort(self: Box<Self>) -> Result<(), Error>;
}
