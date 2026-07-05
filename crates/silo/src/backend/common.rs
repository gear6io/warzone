//! Shared Catalog/Table plumbing used by every [`super::DestinationWriter`]
//! impl. Backends differ only in which `StorageFactory` + storage props they
//! hand to [`build_catalog`]; everything past "have a `Catalog`" (namespace
//! bootstrap, table load/create, Parquet write, fast-append commit) is
//! identical, so it lives here once instead of being duplicated per backend.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use iceberg::io::StorageFactory;
use iceberg::spec::{DataFileFormat, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use errors::{Code, Error};

use crate::config::CatalogConfig;
use crate::{wrap_iceberg, StreamId};

static CODE_CATALOG_LOAD_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("catalog_load_failed"));
static CODE_INVALID_STREAM_IDENTIFIER: LazyLock<Code> = LazyLock::new(|| Code::must_new("invalid_stream_identifier"));
static CODE_NAMESPACE_SETUP_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("namespace_setup_failed"));
static CODE_TABLE_LOAD_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("table_load_failed"));
static CODE_UNKNOWN_STREAM: LazyLock<Code> = LazyLock::new(|| Code::must_new("unknown_stream"));
static CODE_SCHEMA_CONVERSION_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("schema_conversion_failed"));
static CODE_BATCH_RETAG_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("batch_retag_failed"));
static CODE_PARQUET_WRITE_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("parquet_write_failed"));
static CODE_TABLE_COMMIT_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("table_commit_failed"));
static CODE_SCHEMA_EVOLUTION_UNSUPPORTED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("schema_evolution_unsupported"));
static CODE_CATALOG_CHECK_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("catalog_check_failed"));

/// Builds the `Catalog` for one destination from its `CatalogConfig` plus
/// the backend-supplied storage factory/props (S3 credentials, or nothing
/// for local fs/memory). REST-only for v1: adding Glue/SQL/HMS later is a
/// new match arm here, no trait changes elsewhere.
pub(crate) async fn build_catalog(
    catalog: &CatalogConfig,
    storage_factory: Arc<dyn StorageFactory>,
    mut storage_props: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>, Error> {
    match catalog {
        CatalogConfig::Rest { uri, warehouse, props } => {
            storage_props.extend(props.clone());
            storage_props.insert("uri".to_string(), uri.clone());
            storage_props.insert("warehouse".to_string(), warehouse.clone());
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(storage_factory)
                .load("silo", storage_props)
                .await
                .map_err(|e| wrap_iceberg(e, CODE_CATALOG_LOAD_FAILED.clone(), "failed to load REST catalog"))?;
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Memory { warehouse } => {
            storage_props.insert(iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone());
            let catalog = iceberg::memory::MemoryCatalogBuilder::default()
                .with_storage_factory(storage_factory)
                .load("silo", storage_props)
                .await
                .map_err(|e| wrap_iceberg(e, CODE_CATALOG_LOAD_FAILED.clone(), "failed to load memory catalog"))?;
            Ok(Arc::new(catalog))
        }
    }
}

/// Shared `Catalog` + per-stream `Table` bookkeeping and the actual
/// Iceberg-format work (load/create table, write a Parquet data file,
/// fast-append it via a transaction commit). Every [`super::DestinationWriter`]
/// backend wraps one of these; they differ only in how `build_catalog` was
/// called to produce `catalog`.
pub(crate) struct IcebergBackend {
    name: String,
    catalog: Arc<dyn Catalog>,
    tables: HashMap<StreamId, Table>,
}

impl IcebergBackend {
    pub(crate) fn new(name: String, catalog: Arc<dyn Catalog>) -> Self {
        Self { name, catalog, tables: HashMap::new() }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    fn ident(stream: &StreamId) -> Result<TableIdent, Error> {
        let ns = NamespaceIdent::from_strs(stream.namespace.iter()).map_err(|e| {
            wrap_iceberg(e, CODE_INVALID_STREAM_IDENTIFIER.clone(), format!("invalid namespace for stream {stream:?}"))
        })?;
        Ok(TableIdent::new(ns, stream.table.clone()))
    }

    pub(crate) async fn ensure_table(
        &mut self,
        stream: &StreamId,
        schema: &IcebergSchema,
    ) -> Result<IcebergSchema, Error> {
        let ident = Self::ident(stream)?;

        let namespace_setup_failed =
            |e| wrap_iceberg(e, CODE_NAMESPACE_SETUP_FAILED.clone(), "failed to ensure namespace exists");
        if !self.catalog.namespace_exists(ident.namespace()).await.map_err(namespace_setup_failed)? {
            self.catalog
                .create_namespace(ident.namespace(), HashMap::new())
                .await
                .map_err(namespace_setup_failed)?;
        }

        let table_load_failed =
            |e| wrap_iceberg(e, CODE_TABLE_LOAD_FAILED.clone(), "failed to load or create table");
        let table = if self.catalog.table_exists(&ident).await.map_err(table_load_failed)? {
            self.catalog.load_table(&ident).await.map_err(table_load_failed)?
        } else {
            let creation = TableCreation::builder()
                .name(ident.name().to_string())
                .schema(schema.clone())
                .build();
            self.catalog
                .create_table(ident.namespace(), creation)
                .await
                .map_err(table_load_failed)?
        };

        let current_schema = table.metadata().current_schema().as_ref().clone();
        self.tables.insert(stream.clone(), table);
        Ok(current_schema)
    }

    pub(crate) async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), Error> {
        let table = self
            .tables
            .get(stream)
            .ok_or_else(|| Error::new_not_found(CODE_UNKNOWN_STREAM.clone(), format!("unknown stream {stream:?}")))?
            .clone();

        // The incoming batch's Arrow schema generally has no Iceberg
        // field-id metadata (it comes from the source, not from this
        // table), but iceberg-rust's Parquet writer requires each column to
        // carry a `PARQUET:field_id` matching the table's schema. Re-tag the
        // batch against the table's own current schema (the source of
        // truth for field ids) rather than trying to invent ids ourselves.
        let arrow_schema_with_ids = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(|e| {
                wrap_iceberg(e, CODE_SCHEMA_CONVERSION_FAILED.clone(), "failed to convert table schema to Arrow")
            })?,
        );
        let batch = RecordBatch::try_new(arrow_schema_with_ids, batch.columns().to_vec()).map_err(|e| {
            Error::wrap_internal(
                e,
                CODE_BATCH_RETAG_FAILED.clone(),
                "failed to retag record batch with table field ids",
            )
        })?;

        // ponytail: one Parquet file + one commit per write() call — simplest
        // correct thing. Batch multiple writes into one transaction/file when
        // commit-frequency or small-file-count actually becomes a problem.
        let parquet_write_failed =
            |e| wrap_iceberg(e, CODE_PARQUET_WRITE_FAILED.clone(), "failed to write Parquet data file");
        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).map_err(parquet_write_failed)?;
        let file_name_generator =
            DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut writer = data_file_writer_builder.build(None).await.map_err(parquet_write_failed)?;
        writer.write(batch).await.map_err(parquet_write_failed)?;
        let data_files = writer.close().await.map_err(parquet_write_failed)?;

        let table_commit_failed =
            |e| wrap_iceberg(e, CODE_TABLE_COMMIT_FAILED.clone(), "failed to commit fast-append transaction");
        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx).map_err(table_commit_failed)?;
        let updated_table = tx.commit(self.catalog.as_ref()).await.map_err(table_commit_failed)?;

        self.tables.insert(stream.clone(), updated_table);
        Ok(())
    }

    /// iceberg-rust 0.9.1 exposes no public way to evolve an existing table's
    /// schema: `Transaction` has no `update_schema`, and `TableCommit`'s
    /// builder is private ("dangerous and error-prone to construct
    /// directly"), so external crates cannot apply `TableUpdate::AddSchema`
    /// themselves. Revisit once upstream adds a `Transaction` schema action.
    pub(crate) fn evolve_schema(&self, stream: &StreamId, fields: Vec<String>) -> Result<IcebergSchema, Error> {
        Err(Error::new_unsupported(
            CODE_SCHEMA_EVOLUTION_UNSUPPORTED.clone(),
            format!(
                "schema evolution for stream {stream:?} is not supported by iceberg-rust 0.9.1 \
                 (no public Transaction::update_schema); new/changed fields: {fields:?}"
            ),
        ))
    }

    pub(crate) fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        self.tables.remove(stream);
        Ok(())
    }

    pub(crate) async fn check(&self) -> Result<(), Error> {
        self.catalog
            .list_namespaces(None)
            .await
            .map_err(|e| wrap_iceberg(e, CODE_CATALOG_CHECK_FAILED.clone(), "connectivity/permissions check failed"))?;
        Ok(())
    }
}
