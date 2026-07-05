//! Standalone Parquet-writing component for one streaming write session.
//! Wraps iceberg-rust's own Parquet writer chain (`ParquetWriterBuilder` ->
//! `RollingFileWriterBuilder` -> `DataFileWriterBuilder`), which is already
//! storage-agnostic: it targets `iceberg::io::FileWrite`, an async
//! write/close sink backed by whichever `Storage`/`StorageFactory` the
//! owning `Table`'s `FileIO` was built with (local filesystem or S3/OpenDAL —
//! see `backend/filesystem.rs` and `backend/s3.rs`). This component doesn't
//! reimplement that abstraction, it just gives it a session-shaped API:
//! many `write()` calls feeding one open file, then either `finish()`
//! (finalize into `DataFile`s for a commit) or `abort()` (delete the
//! partial file, no commit).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use errors::{Code, Error};

use crate::wrap_iceberg;

static CODE_SCHEMA_CONVERSION_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("schema_conversion_failed"));
static CODE_PARQUET_WRITE_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("parquet_write_failed"));
static CODE_BATCH_RETAG_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("batch_retag_failed"));
static CODE_PARQUET_ABORT_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("parquet_abort_failed"));

/// `DefaultFileNameGenerator`'s counter starts at 0 per instance and we
/// build a fresh generator per session — without a distinguishing suffix,
/// every session for the same stream would produce the identical filename
/// ("data-00000.parquet"), silently colliding with a previous session's
/// already-committed file. This process-wide counter gives every session a
/// unique suffix regardless of which stream/table it belongs to.
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

type Writer = iceberg::writer::base_writer::data_file_writer::DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// One Parquet file for one write session. A roll to a second physical file
/// is made impossible (`target_file_size = usize::MAX`) so that
/// `current_file_path()` always names the only file this session will ever
/// produce — that's what makes `abort()`'s cleanup exact rather than
/// "correct only if no roll happened". One session (bounded by an explicit
/// finish signal from the caller) mapping to one file is the natural unit
/// here; revisit if a single session's stream turns out to run long enough
/// for unbounded file size to become a real problem.
pub(crate) struct ParquetFileWriter {
    inner: Writer,
    file_io: FileIO,
    arrow_schema_with_ids: Arc<arrow_schema::Schema>,
    wrote_any: bool,
}

impl ParquetFileWriter {
    pub(crate) async fn new(table: &Table) -> Result<Self, Error> {
        let arrow_schema_with_ids = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(|e| {
                wrap_iceberg(e, CODE_SCHEMA_CONVERSION_FAILED.clone(), "failed to convert table schema to Arrow")
            })?,
        );

        let parquet_write_failed =
            |e| wrap_iceberg(e, CODE_PARQUET_WRITE_FAILED.clone(), "failed to open Parquet data file writer");
        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).map_err(parquet_write_failed)?;
        let session_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let file_name_generator =
            DefaultFileNameGenerator::new("data".to_string(), Some(format!("{session_id:x}")), DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            usize::MAX,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let inner = data_file_writer_builder.build(None).await.map_err(parquet_write_failed)?;

        Ok(Self { inner, file_io: table.file_io().clone(), arrow_schema_with_ids, wrote_any: false })
    }

    /// Write one record batch to the open file, retagged against the
    /// schema captured at session start (see the comment on
    /// `IcebergBackend::write` history — iceberg-rust's Parquet writer
    /// requires each column to carry a `PARQUET:field_id` matching the
    /// table's schema, which incoming batches generally don't have).
    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<(), Error> {
        let batch = RecordBatch::try_new(self.arrow_schema_with_ids.clone(), batch.columns().to_vec()).map_err(
            |e| Error::wrap_internal(e, CODE_BATCH_RETAG_FAILED.clone(), "failed to retag record batch with table field ids"),
        )?;
        self.inner
            .write(batch)
            .await
            .map_err(|e| wrap_iceberg(e, CODE_PARQUET_WRITE_FAILED.clone(), "failed to write Parquet data file"))?;
        self.wrote_any = true;
        Ok(())
    }

    /// Finalize the file and return its `DataFile`s, ready to be added to a
    /// fast-append transaction. No-op-safe to call even if `write()` was
    /// never invoked (closes an empty file).
    pub(crate) async fn finish(mut self) -> Result<Vec<DataFile>, Error> {
        self.inner
            .close()
            .await
            .map_err(|e| wrap_iceberg(e, CODE_PARQUET_WRITE_FAILED.clone(), "failed to finalize Parquet data file"))
    }

    /// Cancel the session: delete the partial file rather than finalizing
    /// it. No Iceberg transaction/commit is ever touched by this path.
    pub(crate) async fn abort(self) -> Result<(), Error> {
        if !self.wrote_any {
            // Nothing was ever opened — `current_file_path()` would panic
            // (it unwraps the not-yet-initialized inner file writer).
            return Ok(());
        }
        let path = self.inner.current_file_path();
        // Drop `self.inner` without calling `close()`: no footer flush for
        // a file we're about to discard.
        let abort_failed =
            |e| wrap_iceberg(e, CODE_PARQUET_ABORT_FAILED.clone(), "failed to delete aborted Parquet data file");
        if self.file_io.exists(&path).await.map_err(abort_failed)? {
            self.file_io.delete(&path).await.map_err(abort_failed)?;
        }
        Ok(())
    }
}
