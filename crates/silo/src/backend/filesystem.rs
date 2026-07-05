//! Local filesystem backend (`file://`), using iceberg-rust's built-in
//! `LocalFsStorageFactory` — a genuinely distinct backend from S3/MinIO,
//! unlike those two which share one implementation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::Schema as IcebergSchema;

use super::common::{build_catalog, IcebergBackend};
use super::DestinationWriter;
use crate::config::{CatalogConfig, StorageConfig};
use crate::{SinkError, StreamId};

pub struct FilesystemDestinationWriter {
    inner: IcebergBackend,
}

impl FilesystemDestinationWriter {
    pub async fn new(name: String, catalog: &CatalogConfig, storage: StorageConfig) -> Result<Self, SinkError> {
        let StorageConfig::FileSystem { root_path: _ } = storage else {
            return Err(SinkError::Other(format!(
                "destination '{name}': FilesystemDestinationWriter requires a FileSystem storage config"
            )));
        };
        // `root_path` isn't threaded into props: the warehouse location
        // (a `file://...` path) comes from `CatalogConfig`, same as the S3
        // backend's `bucket` — see the comment in backend/s3.rs.

        let catalog = build_catalog(catalog, Arc::new(LocalFsStorageFactory), HashMap::new()).await?;
        Ok(Self { inner: IcebergBackend::new(name, catalog) })
    }
}

#[async_trait]
impl DestinationWriter for FilesystemDestinationWriter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn ensure_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, SinkError> {
        self.inner.ensure_table(stream, schema).await
    }

    async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), SinkError> {
        self.inner.write(stream, batch).await
    }

    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<IcebergSchema, SinkError> {
        let fields = new_schema.as_struct().fields().iter().map(|f| f.name.clone()).collect();
        self.inner.evolve_schema(stream, fields)
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), SinkError> {
        self.inner.close(stream)
    }

    async fn check(&self) -> Result<(), SinkError> {
        self.inner.check().await
    }
}
