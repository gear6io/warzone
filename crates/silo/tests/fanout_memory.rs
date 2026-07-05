//! No-infra tests: `MemoryCatalog` (in-memory metadata) + `LocalFsStorage`
//! (`file://` on a tempdir) exercises the full
//! `TableSink -> Destination -> DestinationWriter` path without any real
//! cloud/service dependency.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use errors::{Code, Error};
use iceberg::spec::Schema as IcebergSchema;
use silo::backend::filesystem::FilesystemDestinationWriter;
use silo::backend::DestinationWriter;
use silo::config::CatalogConfig;
use silo::destination::{Destination, MultiDestination, SingleDestination};
use silo::ingest::{IcebergTableSink, TableSink};
use silo::StreamId;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
}

fn memory_catalog_config(warehouse_dir: &Path) -> CatalogConfig {
    CatalogConfig::Memory { warehouse: format!("file://{}", warehouse_dir.display()) }
}

/// Recursively counts files with the given extension under `dir`.
fn count_files_with_ext(dir: &Path, ext: &str) -> usize {
    if !dir.exists() {
        return 0;
    }
    let mut count = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            count += count_files_with_ext(&path, ext);
        } else if path.extension().is_some_and(|e| e == ext) {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn single_destination_writes_a_parquet_file_to_disk() {
    let warehouse = tempfile::tempdir().unwrap();
    let catalog_cfg = memory_catalog_config(warehouse.path());
    let storage_cfg = silo::config::StorageConfig::FileSystem { root_path: warehouse.path().display().to_string() };

    let writer = FilesystemDestinationWriter::new("primary".into(), &catalog_cfg, storage_cfg).await.unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))));

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    let iceberg_schema = silo::ingest::schema::to_iceberg_schema(batch.schema().as_ref()).unwrap();

    sink.setup(&stream, &iceberg_schema).await.unwrap();
    sink.write(&stream, batch).await.unwrap();
    sink.close(&stream).await.unwrap();

    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1);
}

#[tokio::test]
async fn multi_destination_fans_out_to_both_backends() {
    let warehouse_a = tempfile::tempdir().unwrap();
    let warehouse_b = tempfile::tempdir().unwrap();

    let writer_a = FilesystemDestinationWriter::new(
        "a".into(),
        &memory_catalog_config(warehouse_a.path()),
        silo::config::StorageConfig::FileSystem { root_path: warehouse_a.path().display().to_string() },
    )
    .await
    .unwrap();
    let writer_b = FilesystemDestinationWriter::new(
        "b".into(),
        &memory_catalog_config(warehouse_b.path()),
        silo::config::StorageConfig::FileSystem { root_path: warehouse_b.path().display().to_string() },
    )
    .await
    .unwrap();

    let mut sink =
        IcebergTableSink::new(Box::new(MultiDestination::new(vec![Box::new(writer_a), Box::new(writer_b)])));

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    let iceberg_schema = silo::ingest::schema::to_iceberg_schema(batch.schema().as_ref()).unwrap();

    sink.setup(&stream, &iceberg_schema).await.unwrap();
    sink.write(&stream, batch).await.unwrap();
    sink.close(&stream).await.unwrap();

    assert_eq!(count_files_with_ext(warehouse_a.path(), "parquet"), 1);
    assert_eq!(count_files_with_ext(warehouse_b.path(), "parquet"), 1);
}

/// Test double that always fails `write`, used to verify fail-fast surfacing
/// and that a sibling destination's independent commit isn't undone.
struct FailingWriter;

#[async_trait]
impl DestinationWriter for FailingWriter {
    fn name(&self) -> &str {
        "failing"
    }

    async fn ensure_table(&mut self, _stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        Ok(schema.clone())
    }

    async fn write(&mut self, _stream: &StreamId, _batch: &RecordBatch) -> Result<(), Error> {
        Err(Error::new_internal(Code::INTERNAL, "boom"))
    }

    async fn evolve_schema(&mut self, _stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        Ok(schema.clone())
    }

    async fn close(&mut self, _stream: &StreamId) -> Result<(), Error> {
        Ok(())
    }

    async fn check(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::test]
async fn multi_destination_fail_fast_does_not_undo_sibling_commit() {
    let warehouse = tempfile::tempdir().unwrap();
    let good_writer = FilesystemDestinationWriter::new(
        "good".into(),
        &memory_catalog_config(warehouse.path()),
        silo::config::StorageConfig::FileSystem { root_path: warehouse.path().display().to_string() },
    )
    .await
    .unwrap();

    let mut destination = MultiDestination::new(vec![Box::new(good_writer), Box::new(FailingWriter)]);

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    let iceberg_schema = silo::ingest::schema::to_iceberg_schema(batch.schema().as_ref()).unwrap();

    destination.ensure_table(&stream, &iceberg_schema).await.unwrap();

    let result = destination.write(&stream, &batch).await;
    assert!(result.is_err(), "expected the failing destination to surface an error");

    // The healthy destination's independent commit still landed on disk —
    // fail-fast surfaces the error, it doesn't roll anything back.
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1);
}
