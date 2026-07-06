//! Shared Catalog/Table plumbing used by every [`super::DestinationWriter`]
//! impl. Backends differ only in which `StorageFactory` + storage props they
//! hand to [`build_catalog`]; everything past "have a `Catalog`" (namespace
//! bootstrap, table load/create, streaming Parquet write session,
//! fast-append commit) is identical, so it lives here once instead of being
//! duplicated per backend.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arrow_array::RecordBatch;
use iceberg::io::StorageFactory;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;

use errors::{Code, Error};

use super::parquet_writer::ParquetFileWriter;
use super::WriteSession;
use crate::config::CatalogConfig;
use crate::{wrap_iceberg, StreamId};
use async_trait::async_trait;

static CODE_CATALOG_LOAD_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("catalog_load_failed"));
static CODE_INVALID_STREAM_IDENTIFIER: LazyLock<Code> = LazyLock::new(|| Code::must_new("invalid_stream_identifier"));
static CODE_NAMESPACE_SETUP_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("namespace_setup_failed"));
static CODE_TABLE_LOAD_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("table_load_failed"));
static CODE_UNKNOWN_STREAM: LazyLock<Code> = LazyLock::new(|| Code::must_new("unknown_stream"));
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
    tables: HashMap<StreamId, Arc<Mutex<Table>>>,
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
        self.tables.insert(stream.clone(), Arc::new(Mutex::new(table)));
        Ok(current_schema)
    }

    pub(crate) async fn begin_write(&mut self, stream: &StreamId) -> Result<IcebergWriteSession, Error> {
        let cell = self
            .tables
            .get(stream)
            .ok_or_else(|| Error::new_not_found(CODE_UNKNOWN_STREAM.clone(), format!("unknown stream {stream:?}")))?
            .clone();
        let table = cell.lock().unwrap().clone();
        let writer = ParquetFileWriter::new(&table).await?;
        Ok(IcebergWriteSession { catalog: self.catalog.clone(), table, table_cell: cell, writer })
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

/// One streaming write session against one Iceberg table: a
/// [`ParquetFileWriter`] to accumulate records, plus what's needed to
/// fast-append it on `commit` — the `Table` snapshot the session was
/// opened against, the catalog to commit against, and the shared
/// per-stream cell to write the post-commit `Table` back into (so the
/// *next* session for this stream doesn't build its transaction against
/// stale metadata and get rejected as a conflict).
pub(crate) struct IcebergWriteSession {
    catalog: Arc<dyn Catalog>,
    table: Table,
    table_cell: Arc<Mutex<Table>>,
    writer: ParquetFileWriter,
}

#[async_trait]
impl WriteSession for IcebergWriteSession {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), Error> {
        self.writer.write(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        let data_files = self.writer.finish().await?;

        let table_commit_failed =
            |e| wrap_iceberg(e, CODE_TABLE_COMMIT_FAILED.clone(), "failed to commit fast-append transaction");
        let tx = Transaction::new(&self.table);
        let tx = tx.fast_append().add_data_files(data_files).apply(tx).map_err(table_commit_failed)?;
        let updated_table = tx.commit(self.catalog.as_ref()).await.map_err(table_commit_failed)?;

        *self.table_cell.lock().unwrap() = updated_table;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<(), Error> {
        self.writer.abort().await
    }
}
