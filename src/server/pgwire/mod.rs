mod copy;
mod ddl;
mod error;
mod handlers;
mod results;
mod server;

use std::sync::LazyLock;

use errors::{Code, Error};
use silo::StreamId;

use crate::parser::ast::ObjectName;

pub use server::serve;

static CODE_MISSING_TABLE_NAME: LazyLock<Code> = LazyLock::new(|| Code::must_new("missing_table_name"));

/// `demo.events` -> namespace `["demo"]`, table `events`. Shared by the two
/// statements that name a table: `CREATE TABLE` and `COPY ... FROM STDIN`.
fn stream_id(name: &ObjectName) -> Result<StreamId, Error> {
    let mut parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    let table = parts.pop().ok_or_else(|| {
        Error::new_invalid_input(CODE_MISSING_TABLE_NAME.clone(), "statement has no table name".to_string())
    })?;
    Ok(StreamId::new(parts, table))
}
