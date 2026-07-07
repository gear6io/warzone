use std::sync::Arc;

use errors::{Code, Error};
use silo::backend::filesystem::FilesystemDestinationWriter;
use silo::backend::s3::S3DestinationWriter;
use silo::backend::DestinationWriter;
use silo::config::{DestinationConfig, SinkConfig, StorageConfig};
use silo::destination::{Destination, MultiDestination, SingleDestination};
use silo::ingest::IcebergTableSink;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub sink: Arc<Mutex<IcebergTableSink>>,
}

async fn build_writer(config: &DestinationConfig) -> Result<Box<dyn DestinationWriter>, Error> {
    match config.storage.clone().into_resolved_s3() {
        StorageConfig::FileSystem { .. } => {
            let writer = FilesystemDestinationWriter::new(config.name.clone(), &config.catalog, config.storage.clone()).await?;
            Ok(Box::new(writer))
        }
        StorageConfig::S3 { .. } => {
            let writer = S3DestinationWriter::new(config.name.clone(), &config.catalog, config.storage.clone()).await?;
            Ok(Box::new(writer))
        }
        StorageConfig::Memory | StorageConfig::Minio { .. } => Err(Error::new_invalid_input(
            Code::must_new("invalid_destination_storage"),
            format!("destination '{}': storage backend is not valid for a running server", config.name),
        )),
    }
}

pub async fn build_sink(config: &SinkConfig) -> Result<IcebergTableSink, Error> {
    let mut writers: Vec<Box<dyn DestinationWriter>> = Vec::with_capacity(config.destinations.len());
    for destination in &config.destinations {
        writers.push(build_writer(destination).await?);
    }

    let destination: Box<dyn Destination> = if writers.len() == 1 {
        Box::new(SingleDestination::new(writers.into_iter().next().unwrap()))
    } else {
        Box::new(MultiDestination::new(writers))
    };

    Ok(IcebergTableSink::new(destination))
}
