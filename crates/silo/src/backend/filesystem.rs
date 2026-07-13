//! Local filesystem backend (`file://`), using iceberg-rust's built-in
//! `LocalFsStorageFactory` — a genuinely distinct backend from S3/MinIO,
//! unlike those two which share one implementation.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::Schema as IcebergSchema;

use errors::{Code, Error};

use super::common::{build_catalog, IcebergBackend};
use super::{DestinationWriter, WriteSession};
use crate::config::{CatalogConfig, StorageConfig};
use crate::StreamId;

static CODE_INVALID_STORAGE_CONFIG: LazyLock<Code> = LazyLock::new(|| Code::must_new("invalid_storage_config"));

pub struct FilesystemDestinationWriter {
    inner: IcebergBackend,
}

impl FilesystemDestinationWriter {
    pub async fn new(name: String, catalog: &CatalogConfig, storage: StorageConfig) -> Result<Self, Error> {
        let StorageConfig::FileSystem { root_path: _ } = storage else {
            return Err(Error::new_invalid_input(
                CODE_INVALID_STORAGE_CONFIG.clone(),
                format!("destination '{name}': FilesystemDestinationWriter requires a FileSystem storage config"),
            ));
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

    async fn load_table(&mut self, stream: &StreamId) -> Result<Option<IcebergSchema>, Error> {
        self.inner.load_table(stream).await
    }

    async fn create_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        self.inner.create_table(stream, schema).await
    }

    async fn begin_write(&mut self, stream: &StreamId) -> Result<Box<dyn WriteSession>, Error> {
        Ok(Box::new(self.inner.begin_write(stream).await?))
    }

    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        let fields = new_schema.as_struct().fields().iter().map(|f| f.name.clone()).collect();
        self.inner.evolve_schema(stream, fields)
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.inner.close(stream)
    }

    async fn check(&self) -> Result<(), Error> {
        self.inner.check().await
    }
}
