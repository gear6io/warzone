use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;

use errors::Error;

use super::{Destination, DestinationSession};
use crate::backend::{DestinationWriter, WriteSession};
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
    async fn load_table(&mut self, stream: &StreamId) -> Result<Option<IcebergSchema>, Error> {
        self.writer.load_table(stream).await
    }

    async fn create_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        self.writer.create_table(stream, schema).await
    }

    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn DestinationSession>, Error> {
        let session = self.writer.begin_write(stream).await?;
        Ok(Box::new(SingleWriteSession { session }))
    }

    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<(), Error> {
        self.writer.evolve_schema(stream, new_schema).await.map(|_| ())
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.writer.close(stream).await
    }
}

struct SingleWriteSession {
    session: Box<dyn WriteSession>,
}

#[async_trait]
impl DestinationSession for SingleWriteSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        self.session.write(batch.clone()).await
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        self.session.commit().await
    }

    async fn abort(self: Box<Self>) -> Result<(), Error> {
        self.session.abort().await
    }
}
