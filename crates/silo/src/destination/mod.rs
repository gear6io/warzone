pub mod multi;
pub mod single;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

pub use multi::MultiDestination;
pub use single::SingleDestination;

use crate::{SinkError, StreamId};

/// Layer 2: fans a stream of record batches out to one ([`SingleDestination`])
/// or several ([`MultiDestination`]) independently-configured backends.
/// Callers (Layer 1, [`crate::ingest::TableSink`]) don't need to know which.
#[async_trait]
pub trait Destination: Send + Sync {
    async fn ensure_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<(), SinkError>;
    async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), SinkError>;
    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<(), SinkError>;
    async fn close(&mut self, stream: &StreamId) -> Result<(), SinkError>;
}
