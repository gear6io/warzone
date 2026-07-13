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
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use silo::backend::filesystem::FilesystemDestinationWriter;
use silo::backend::{DestinationWriter, WriteSession};
use silo::config::CatalogConfig;
use silo::destination::{Destination, MultiDestination, SingleDestination};
use silo::ingest::{Column, ColumnType, IcebergTableSink, TableSink, Value};
use silo::StreamId;

/// The one table every test registers: `id INT NOT NULL`.
fn columns() -> Vec<Column> {
    vec![Column::new("id", ColumnType::Int64, true)]
}

fn rows() -> Vec<Vec<Value>> {
    vec![vec![Value::Int64(1)], vec![Value::Int64(2)], vec![Value::Int64(3)]]
}

/// The layer-2 tests drive `Destination` directly, below the row API, so they
/// still speak Arrow/Iceberg — that is exactly the boundary they are testing.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
}

fn iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(vec![NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into()])
        .build()
        .unwrap()
}

fn memory_catalog_config(warehouse_dir: &Path) -> CatalogConfig {
    CatalogConfig::Memory { warehouse: format!("file://{}", warehouse_dir.display()) }
}

fn fs_storage(dir: &Path) -> silo::config::StorageConfig {
    silo::config::StorageConfig::FileSystem { root_path: dir.display().to_string() }
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
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 10_000);

    let stream = StreamId::new(["ns"], "events");
    sink.register(&stream, &columns()).await.unwrap();

    let mut session = sink.begin_write(&stream).await.unwrap();
    for row in rows() {
        session.push(row).await.unwrap();
    }
    session.commit().await.unwrap();
    sink.close(&stream).await.unwrap();

    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1);
}

/// The invariant the whole row API exists to guarantee: many pushes, spanning
/// several internal batch flushes, still land as exactly ONE Parquet file and
/// ONE snapshot. If `push` ever started committing per batch, this would break.
#[tokio::test]
async fn many_rows_across_several_flushes_still_make_one_file() {
    let warehouse = tempfile::tempdir().unwrap();
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    // batch_size 10 over 95 rows => 9 internal flushes plus a remainder at commit.
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 10);

    let stream = StreamId::new(["ns"], "events");
    sink.register(&stream, &columns()).await.unwrap();

    let mut session = sink.begin_write(&stream).await.unwrap();
    for i in 0..95 {
        session.push(vec![Value::Int64(i)]).await.unwrap();
    }
    session.commit().await.unwrap();

    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1);
}

#[tokio::test]
async fn registering_the_same_table_twice_is_rejected() {
    let warehouse = tempfile::tempdir().unwrap();
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 10_000);

    let stream = StreamId::new(["ns"], "events");
    sink.register(&stream, &columns()).await.unwrap();
    assert!(sink.register(&stream, &columns()).await.is_err());
}

/// Strictness: ingest never creates a table. Without a `register`, `begin_write`
/// must refuse rather than conjure a schema from thin air.
#[tokio::test]
async fn begin_write_on_an_unregistered_table_is_rejected() {
    let warehouse = tempfile::tempdir().unwrap();
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 10_000);

    let stream = StreamId::new(["ns"], "nope");
    assert!(sink.schema(&stream).await.unwrap().is_none());
    assert!(sink.begin_write(&stream).await.is_err());
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 0);
}

/// `schema()` reads back what `register()` wrote — if these ever disagreed,
/// callers would coerce rows against a shape the table does not have.
#[tokio::test]
async fn the_registered_schema_is_what_reads_back() {
    let warehouse = tempfile::tempdir().unwrap();
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 10_000);

    let stream = StreamId::new(["ns"], "events");
    let registered = vec![
        Column::new("id", ColumnType::Int64, true),
        Column::new("name", ColumnType::String, false),
        Column::new("score", ColumnType::Float64, false),
    ];
    sink.register(&stream, &registered).await.unwrap();

    assert_eq!(sink.schema(&stream).await.unwrap().unwrap(), registered);
}

#[tokio::test]
async fn multi_destination_fans_out_to_both_backends() {
    let warehouse_a = tempfile::tempdir().unwrap();
    let warehouse_b = tempfile::tempdir().unwrap();

    let writer_a =
        FilesystemDestinationWriter::new("a".into(), &memory_catalog_config(warehouse_a.path()), fs_storage(warehouse_a.path()))
            .await
            .unwrap();
    let writer_b =
        FilesystemDestinationWriter::new("b".into(), &memory_catalog_config(warehouse_b.path()), fs_storage(warehouse_b.path()))
            .await
            .unwrap();

    let mut sink =
        IcebergTableSink::new(Box::new(MultiDestination::new(vec![Box::new(writer_a), Box::new(writer_b)])), 10_000);

    let stream = StreamId::new(["ns"], "events");
    sink.register(&stream, &columns()).await.unwrap();

    let mut session = sink.begin_write(&stream).await.unwrap();
    for row in rows() {
        session.push(row).await.unwrap();
    }
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

    async fn load_table(&mut self, _stream: &StreamId) -> Result<Option<IcebergSchema>, Error> {
        Ok(Some(iceberg_schema()))
    }

    async fn create_table(&mut self, _stream: &StreamId, schema: &IcebergSchema) -> Result<IcebergSchema, Error> {
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
    let good_writer =
        FilesystemDestinationWriter::new("good".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();

    let mut destination = MultiDestination::new(vec![Box::new(good_writer), Box::new(FailingWriter::new())]);

    let stream = StreamId::new(["ns"], "events");
    let batch = sample_batch();
    destination.create_table(&stream, &iceberg_schema()).await.unwrap();

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
    let writer =
        FilesystemDestinationWriter::new("primary".into(), &memory_catalog_config(warehouse.path()), fs_storage(warehouse.path()))
            .await
            .unwrap();
    // batch_size 1 so a single push actually reaches the Parquet writer — with
    // the default the row would still be buffered and this test would pass
    // vacuously, asserting on a file that was never opened.
    let mut sink = IcebergTableSink::new(Box::new(SingleDestination::new(Box::new(writer))), 1);

    let stream = StreamId::new(["ns"], "events");
    sink.register(&stream, &columns()).await.unwrap();

    let mut session = sink.begin_write(&stream).await.unwrap();
    session.push(vec![Value::Int64(1)]).await.unwrap();

    // The push already landed bytes in a physical (uncommitted) Parquet file —
    // confirm that before aborting, so this test isn't vacuously passing.
    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 1, "push() should have opened a partial file");

    session.abort().await.unwrap();

    assert_eq!(count_files_with_ext(warehouse.path(), "parquet"), 0, "abort() should have deleted the partial file");
}
