use std::collections::HashMap;

use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub destinations: Vec<DestinationConfig>,
    /// Rows buffered before an ingest session flushes them as one Arrow
    /// `RecordBatch` into its Parquet file. The single knob controlling how many
    /// rows are ever held in memory during an ingest — see
    /// [`crate::ingest::IngestSession::push`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    /// User-facing label, e.g. "primary-s3", "local-backup". Used to
    /// identify this destination in fan-out error reporting.
    pub name: String,
    pub catalog: CatalogConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogConfig {
    Rest {
        uri: String,
        warehouse: String,
        #[serde(default, flatten)]
        props: HashMap<String, String>,
    },
    /// In-memory catalog; test-only, never persisted.
    Memory { warehouse: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageConfig {
    S3 {
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
        #[serde(default)]
        path_style: bool,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    },
    /// Ergonomic preset: MinIO is S3-compatible storage at a custom
    /// endpoint with path-style addressing forced on. This resolves into
    /// the same internal S3 storage config as `StorageConfig::S3` with
    /// `endpoint` set and `path_style = true` — there is no separate
    /// MinIO backend implementation.
    Minio {
        bucket: String,
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
    },
    FileSystem { root_path: String },
    /// In-memory storage; test-only, never persisted.
    Memory,
}

impl StorageConfig {
    /// Normalizes the `Minio` preset into the equivalent `S3` config.
    /// `S3`, `FileSystem`, and `Memory` are returned unchanged.
    pub fn into_resolved_s3(self) -> StorageConfig {
        match self {
            StorageConfig::Minio { bucket, endpoint, access_key_id, secret_access_key } => StorageConfig::S3 {
                bucket,
                region: None,
                endpoint: Some(endpoint),
                path_style: true,
                access_key_id: Some(access_key_id),
                secret_access_key: Some(secret_access_key),
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minio_preset_resolves_to_equivalent_s3_config() {
        let minio = StorageConfig::Minio {
            bucket: "warehouse".into(),
            endpoint: "http://localhost:9000".into(),
            access_key_id: "admin".into(),
            secret_access_key: "password".into(),
        }
        .into_resolved_s3();

        let expected = StorageConfig::S3 {
            bucket: "warehouse".into(),
            region: None,
            endpoint: Some("http://localhost:9000".into()),
            path_style: true,
            access_key_id: Some("admin".into()),
            secret_access_key: Some("password".into()),
        };

        assert_eq!(serde_json::to_string(&minio).unwrap(), serde_json::to_string(&expected).unwrap());
    }

    #[test]
    fn sink_config_round_trips_with_multiple_destinations() {
        let json = serde_json::json!({
            "destinations": [
                {
                    "name": "primary-s3",
                    "catalog": { "type": "rest", "uri": "http://localhost:8181", "warehouse": "s3://warehouse" },
                    "storage": { "type": "s3", "bucket": "warehouse", "region": "us-east-1", "endpoint": null, "path_style": false, "access_key_id": null, "secret_access_key": null }
                },
                {
                    "name": "local-backup",
                    "catalog": { "type": "memory", "warehouse": "file:///tmp/warehouse" },
                    "storage": { "type": "file_system", "root_path": "/tmp/warehouse" }
                }
            ]
        });

        let config: SinkConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.destinations.len(), 2);
    }
}
