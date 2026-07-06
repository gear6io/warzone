//! No-infra tests: `MemoryCatalog` (in-memory metadata) + `LocalFsStorage`
//! (`file://` on a tempdir) exercises the full
//! `TableSink -> Destination -> DestinationWriter` streaming-session path
//! without any real cloud/service dependency.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use errors::{Code, Error};
use iceberg::spec::Schema as IcebergSchema;
use silo::backend::filesystem::FilesystemDestinationWriter;
use silo::backend::{DestinationWriter, WriteSession};
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
    let mut session = sink.begin_write(&stream).await.unwrap();
    session.write(batch).await.unwrap();
    session.commit().await.unwrap();
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
    let mut session = sink.begin_write(&stream).await.unwrap();
    session.write(batch).await.unwrap();
    session.commit().await.unwrap();
    sink.close(&stream).await.unwrap();

    assert_eq!(count_files_with_ext(warehouse_a.path(), "parquet"), 1);
    assert_eq!(count_files_with_ext(warehouse_b.path(), "parquet"), 1);
}

/// Fake session for [`FailingWriter`]: never touches disk, just fails
/// `write` once `should_fail` is set.
struct FailingSession {
    should_fail: bool,
}

#[async_trait]
impl WriteSession for FailingSession {
    async fn write(&mut self, _batch: RecordBatch) -> Result<(), Error> {
        if self.should_fail {
            Err(Error::new_internal(Code::INTERNAL, "boom"))
        } else {
            Ok(())
        }
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }
}

/// Test double whose session succeeds on the first `begin_write` and fails
/// every `write` from the second `begin_write` onward. Used to verify
/// fail-fast surfacing and that an already-committed sibling from an
/// earlier, separate call isn't retroactively undone by a later failure.
struct FailingWriter {
    begin_write_calls: usize,
}

impl FailingWriter {
    fn new() -> Self {
        Self { begin_write_calls: 0 }
    }
}

#[async_trait]
impl DestinationWriter for FailingWriter {
    fn name(&self) -> &str {
        "failing"
    }

    async fn ensure_table(&mut self, _stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
        Ok(schema.clone())
    }

    async fn begin_write(&mut self, _stream: &StreamId) -> Result<Box<dyn WriteSession>, Error> {
        self.begin_write_calls += 1;
        Ok(Box::new(FailingSession { should_fail: self.begin_write_calls > 1 }))
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

    let mut destination = MultiDestination::new(vec![Box::new(good_writer), Box::new(FailingWriter::new())]);

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    let iceberg_schema = silo::ingest::schema::to_iceberg_schema(batch.schema().as_ref()).unwrap();

    destination.ensure_table(&stream, &iceberg_schema).await.unwrap();

    // Call #1: FailingWriter's first session succeeds, so both destinations
    // commit. This durable commit must never be undone by a later failure.
    let mut session = destination.begin_write(&stream).await.unwrap();
    session.write(&batch).await.unwrap();
    session.commit().await.unwrap();
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1, "call #1 should commit exactly one file");

    // Call #2: FailingWriter's second session fails on write. The good
    // destination's same-call partial file must be cleaned up by abort.
    let mut session = destination.begin_write(&stream).await.unwrap();
    let write_result = session.write(&batch).await;
    assert!(write_result.is_err(), "expected the failing destination to surface an error");
    session.abort().await.unwrap();

    // Exactly one file survives: call #1's commit wasn't undone, and call
    // #2's aborted partial file didn't leak.
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1);
}

#[tokio::test]
async fn ingest_session_abort_leaves_no_partial_parquet_file() {
    let warehouse = tempfile::tempdir().unwrap();
    let catalog_cfg = memory_catalog_config(warehouse.path());
    let storage_cfg = silo::config::StorageConfig::FileSystem { root_path: warehouse.path().display().to_string() };

    let writer = FilesystemDestinationWriter::new("primary".into(), &catalog_cfg, storage_cfg).await.unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))));

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    let iceberg_schema = silo::ingest::schema::to_iceberg_schema(batch.schema().as_ref()).unwrap();

    sink.setup(&stream, &iceberg_schema).await.unwrap();
    let mut session = sink.begin_write(&stream).await.unwrap();
    session.write(batch).await.unwrap();

    // The write already landed bytes in a physical (uncommitted) Parquet
    // file — confirm that before aborting, so this test isn't vacuously
    // passing because no file was ever created.
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1, "write() should have opened a partial file");

    session.abort().await.unwrap();

    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 0, "abort() should have deleted the partial file");
}
