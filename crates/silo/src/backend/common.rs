//! Shared Catalog/Table plumbing used by every [`super::DestinationWriter`]
//! impl. Backends differ only in which `StorageFactory` + storage props they
//! hand to [`build_catalog`]; everything past "have a `Catalog`" (namespace
//! bootstrap, table load/create, Parquet write, fast-append commit) is
//! identical, so it lives here once instead of being duplicated per backend.

use std::collections::HashMap;
use std::sync::Arc;

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

use crate::config::CatalogConfig;
use crate::{SinkError, StreamId};

/// Builds the `Catalog` for one destination from its `CatalogConfig` plus
/// the backend-supplied storage factory/props (S3 credentials, or nothing
/// for local fs/memory). REST-only for v1: adding Glue/SQL/HMS later is a
/// new match arm here, no trait changes elsewhere.
pub(crate) async fn build_catalog(
    catalog: &CatalogConfig,
    storage_factory: Arc<dyn StorageFactory>,
    mut storage_props: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>, SinkError> {
    match catalog {
        CatalogConfig::Rest { uri, warehouse, props } => {
            storage_props.extend(props.clone());
            storage_props.insert("uri".to_string(), uri.clone());
            storage_props.insert("warehouse".to_string(), warehouse.clone());
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(storage_factory)
                .load("silo", storage_props)
                .await?;
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Memory { warehouse } => {
            storage_props.insert(iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone());
            let catalog = iceberg::memory::MemoryCatalogBuilder::default()
                .with_storage_factory(storage_factory)
                .load("silo", storage_props)
                .await?;
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

    fn ident(stream: &StreamId) -> Result<TableIdent, SinkError> {
        let ns = NamespaceIdent::from_strs(stream.namespace.iter())?;
        Ok(TableIdent::new(ns, stream.table.clone()))
    }

    pub(crate) async fn ensure_table(
        &mut self,
        stream: &StreamId,
        schema: &IcebergSchema,
    ) -> Result<IcebergSchema, SinkError> {
        let ident = Self::ident(stream)?;

        if !self.catalog.namespace_exists(ident.namespace()).await? {
            self.catalog.create_namespace(ident.namespace(), HashMap::new()).await?;
        }

        let table = if self.catalog.table_exists(&ident).await? {
            self.catalog.load_table(&ident).await?
        } else {
            let creation = TableCreation::builder()
                .name(ident.name().to_string())
                .schema(schema.clone())
                .build();
            self.catalog.create_table(ident.namespace(), creation).await?
        };

        let current_schema = table.metadata().current_schema().as_ref().clone();
        self.tables.insert(stream.clone(), table);
        Ok(current_schema)
    }

    pub(crate) async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), SinkError> {
        let table = self
            .tables
            .get(stream)
            .ok_or_else(|| SinkError::UnknownStream(stream.clone()))?
            .clone();

        // The incoming batch's Arrow schema generally has no Iceberg
        // field-id metadata (it comes from the source, not from this
        // table), but iceberg-rust's Parquet writer requires each column to
        // carry a `PARQUET:field_id` matching the table's schema. Re-tag the
        // batch against the table's own current schema (the source of
        // truth for field ids) rather than trying to invent ids ourselves.
        let arrow_schema_with_ids =
            Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?);
        let batch = RecordBatch::try_new(arrow_schema_with_ids, batch.columns().to_vec())
            .map_err(|e| SinkError::Other(e.to_string()))?;

        // ponytail: one Parquet file + one commit per write() call — simplest
        // correct thing. Batch multiple writes into one transaction/file when
        // commit-frequency or small-file-count actually becomes a problem.
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
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
        let mut writer = data_file_writer_builder.build(None).await?;
        writer.write(batch).await?;
        let data_files = writer.close().await?;

        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
        let updated_table = tx.commit(self.catalog.as_ref()).await?;

        self.tables.insert(stream.clone(), updated_table);
        Ok(())
    }

    /// See [`SinkError::SchemaEvolutionUnsupported`].
    pub(crate) fn evolve_schema(&self, stream: &StreamId, fields: Vec<String>) -> Result<IcebergSchema, SinkError> {
        Err(SinkError::SchemaEvolutionUnsupported { stream: stream.clone(), fields })
    }

    pub(crate) fn close(&mut self, stream: &StreamId) -> Result<(), SinkError> {
        self.tables.remove(stream);
        Ok(())
    }

    pub(crate) async fn check(&self) -> Result<(), SinkError> {
        self.catalog.list_namespaces(None).await?;
        Ok(())
    }
}
