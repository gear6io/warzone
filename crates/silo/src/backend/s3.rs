//! S3-compatible backend. Also covers MinIO: MinIO is S3-compatible storage
//! at a custom endpoint with path-style addressing forced on, so
//! `StorageConfig::Minio` is resolved into this same `S3` config shape
//! ([`StorageConfig::into_resolved_s3`]) rather than getting a second
//! `DestinationWriter` implementation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::Schema as IcebergSchema;
use iceberg_storage_opendal::OpenDalStorageFactory;

use super::common::{build_catalog, IcebergBackend};
use super::DestinationWriter;
use crate::config::{CatalogConfig, StorageConfig};
use crate::{SinkError, StreamId};

pub struct S3DestinationWriter {
    inner: IcebergBackend,
}

impl S3DestinationWriter {
    pub async fn new(name: String, catalog: &CatalogConfig, storage: StorageConfig) -> Result<Self, SinkError> {
        // `bucket` is not read here: the warehouse path (which encodes the
        // bucket) comes from `CatalogConfig`, not `StorageConfig` — it's kept
        // on `StorageConfig::S3` purely for config-shape clarity/validation.
        let StorageConfig::S3 { bucket: _, region, endpoint, path_style, access_key_id, secret_access_key } =
            storage.into_resolved_s3()
        else {
            return Err(SinkError::Other(format!(
                "destination '{name}': S3DestinationWriter requires an S3 or Minio storage config"
            )));
        };

        let mut props = HashMap::new();
        if let Some(endpoint) = endpoint {
            props.insert(S3_ENDPOINT.to_string(), endpoint);
        }
        if let Some(region) = region {
            props.insert(S3_REGION.to_string(), region);
        }
        if let Some(key) = access_key_id {
            props.insert(S3_ACCESS_KEY_ID.to_string(), key);
        }
        if let Some(secret) = secret_access_key {
            props.insert(S3_SECRET_ACCESS_KEY.to_string(), secret);
        }
        props.insert(S3_PATH_STYLE_ACCESS.to_string(), path_style.to_string());

        let factory = Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        });

        let catalog = build_catalog(catalog, factory, props).await?;
        Ok(Self { inner: IcebergBackend::new(name, catalog) })
    }
}

#[async_trait]
impl DestinationWriter for S3DestinationWriter {
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
