mod attach;
mod convert;
mod error;

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use silo::config::SinkConfig;

use errors::Error;

use self::error::{setup_statement_code, wrap_query, wrap_setup, CODE_EXTENSION_LOAD_FAILED};

pub(crate) struct QueryEngine {
    conn: Arc<Mutex<duckdb::Connection>>,
}

#[derive(Debug)]
pub(crate) struct QueryResult {
    pub(crate) schema: SchemaRef,
    pub(crate) batches: Vec<RecordBatch>,
}

impl QueryEngine {
    /// Opens an in-process DuckDB connection, installs/loads the `iceberg`
    /// and `httpfs` extensions, then attaches one DuckDB catalog per
    /// destination in `config` — the same `SinkConfig` silo's write path
    /// is built from, so the querier reads the tables silo writes.
    pub(crate) async fn new(config: &SinkConfig) -> Result<Self, Error> {
        let statements = attach::attach_statements(&config.destinations)?;
        let conn = duckdb::Connection::open_in_memory()
            .map_err(|e| wrap_setup(e, CODE_EXTENSION_LOAD_FAILED.clone(), "failed to open duckdb connection"))?;
        for statement in statements {
            conn.execute_batch(&statement)
                .map_err(|e| wrap_setup(e, setup_statement_code(&statement), format!("failed to run setup statement: {statement}")))?;
        }
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub(crate) async fn query(&self, sql: &str) -> Result<QueryResult, Error> {
        let conn = self.conn.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn.prepare(&sql).map_err(|e| wrap_query(e, &sql))?;
            let arrow = stmt.query_arrow([]).map_err(|e| wrap_query(e, &sql))?;
            let schema = arrow.get_schema();
            let mut batches = Vec::new();
            for batch in arrow {
                batches.push(convert::convert_batch(&batch)?);
            }
            let schema = convert::convert_schema_ref(&schema)?;
            Ok(QueryResult { schema, batches })
        })
        .await
        .map_err(|e| Error::wrap_internal(e, error::CODE_QUERY_EXECUTION_FAILED.clone(), "query task panicked"))?
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array, StringArray};
    use silo::config::{CatalogConfig, DestinationConfig, StorageConfig};

    use super::*;

    fn memory_config() -> SinkConfig {
        SinkConfig {
            destinations: vec![DestinationConfig {
                name: "local".to_string(),
                catalog: CatalogConfig::Memory { warehouse: "file:///tmp/warehouse".to_string() },
                storage: StorageConfig::Memory,
            }],
        }
    }

    #[tokio::test]
    async fn query_engine_round_trips_a_plain_table_through_arrow_ffi() {
        let engine = QueryEngine::new(&memory_config()).await.expect("engine should build against a memory-only config");

        engine.query("CREATE TABLE t (id BIGINT, name VARCHAR);").await.expect("create table");
        engine.query("INSERT INTO t VALUES (1, 'a'), (2, 'b');").await.expect("insert rows");

        let result = engine.query("SELECT id, name FROM t ORDER BY id;").await.expect("select rows");

        assert_eq!(result.schema.fields().len(), 2);
        assert_eq!(result.schema.field(0).name(), "id");
        assert_eq!(result.schema.field(1).name(), "name");

        let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let batch = &result.batches[0];
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id column should be Int64Array");
        let names = batch.column(1).as_any().downcast_ref::<StringArray>().expect("name column should be StringArray");
        assert_eq!(ids.value(0), 1);
        assert_eq!(names.value(0), "a");
        assert_eq!(ids.value(1), 2);
        assert_eq!(names.value(1), "b");
    }

    #[tokio::test]
    async fn invalid_sql_maps_to_invalid_input() {
        let engine = QueryEngine::new(&memory_config()).await.expect("engine should build against a memory-only config");
        let err = engine.query("SELEC 1").await.expect_err("malformed sql should fail");
        assert!(err.is_type(errors::Type::InvalidInput));
    }
}
