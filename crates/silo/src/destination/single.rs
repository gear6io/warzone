use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

use errors::Error;

use super::Destination;
use crate::backend::DestinationWriter;
use crate::StreamId;

/// One destination, no fan-out.
pub struct SingleDestination {
    writer: Box<dyn DestinationWriter>,
}

impl SingleDestination {
    pub fn new(writer: Box<dyn DestinationWriter>) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl Destination for SingleDestination {
    async fn ensure_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<(), Error> {
        self.writer.ensure_table(stream, schema).await.map(|_| ())
    }

    async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), Error> {
        self.writer.write(stream, batch).await
    }

    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<(), Error> {
        self.writer.evolve_schema(stream, new_schema).await.map(|_| ())
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.writer.close(stream).await
    }
}
