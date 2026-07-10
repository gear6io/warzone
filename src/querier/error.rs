use std::sync::LazyLock;

use errors::{Code, Error};

pub(crate) static CODE_EXTENSION_LOAD_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("extension_load_failed"));
pub(crate) static CODE_ATTACH_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("attach_failed"));
pub(crate) static CODE_SECRET_CREATION_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("secret_creation_failed"));
pub(crate) static CODE_INVALID_SQL: LazyLock<Code> = LazyLock::new(|| Code::must_new("invalid_sql"));
pub(crate) static CODE_QUERY_EXECUTION_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("query_execution_failed"));
pub(crate) static CODE_UNSUPPORTED_CATALOG_PROP: LazyLock<Code> = LazyLock::new(|| Code::must_new("unsupported_catalog_prop"));

pub(crate) fn wrap_query(e: duckdb::Error, sql: &str) -> Error {
    match &e {
        duckdb::Error::DuckDBFailure(_, Some(msg))
            if msg.contains("Parser Error") || msg.contains("Binder Error") || msg.contains("Catalog Error") =>
        {
            Error::wrap_invalid_input(e, CODE_INVALID_SQL.clone(), format!("invalid query: {sql}"))
        }
        _ => Error::wrap_internal(e, CODE_QUERY_EXECUTION_FAILED.clone(), format!("query execution failed: {sql}")),
    }
}

pub(crate) fn wrap_setup(e: duckdb::Error, code: Code, message: impl Into<String>) -> Error {
    Error::wrap_internal(e, code, message)
}

/// Picks the most specific error `Code` for a setup statement, by its kind,
/// so `QueryEngine::new` failures point at "extension load" vs "secret
/// creation" vs "catalog attach" instead of one generic bucket.
pub(crate) fn setup_statement_code(statement: &str) -> Code {
    if statement.starts_with("INSTALL") || statement.starts_with("LOAD") {
        CODE_EXTENSION_LOAD_FAILED.clone()
    } else if statement.starts_with("CREATE SECRET") {
        CODE_SECRET_CREATION_FAILED.clone()
    } else {
        CODE_ATTACH_FAILED.clone()
    }
}
