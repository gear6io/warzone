//! `CREATE TABLE` over the Postgres wire protocol — the registration step.
//!
//! This is the *only* way an Iceberg table comes into being. Ingest
//! ([`super::copy`], the HTTP `/insert` endpoint) never creates one: it looks
//! up the registered schema and coerces its wire values into those column
//! types. Registering once, up front, is what lets us delete every bit of
//! "guess the type from whatever row showed up first" logic.
//!
//! No `ALTER TABLE`: iceberg-rust 0.9.1 exposes no way to evolve an existing
//! table's schema, so we error rather than pretend.

use std::sync::LazyLock;

use errors::{Code, Error};
use silo::ingest::{Column, ColumnType};
use silo::StreamId;

use crate::parser::ast::{ColumnDef, ColumnOption, DataType as SqlType, Statement};

use super::stream_id;

static CODE_NO_COLUMNS: LazyLock<Code> = LazyLock::new(|| Code::must_new("create_table_no_columns"));
static CODE_DDL_UNSUPPORTED: LazyLock<Code> = LazyLock::new(|| Code::must_new("ddl_unsupported"));
static CODE_UNSUPPORTED_TYPE: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("create_table_unsupported_type"));

pub struct CreateTablePlan {
    pub stream: StreamId,
    pub columns: Vec<Column>,
    pub if_not_exists: bool,
}

/// Detects a lone `CREATE TABLE`. `Ok(None)` for anything else (including SQL
/// that fails to parse — let the querier report that), `Err` for a CREATE TABLE
/// we recognize but cannot serve.
pub fn detect(sql: &str) -> Result<Option<CreateTablePlan>, Error> {
    let statements = match crate::parser::parse(sql) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };
    let [Statement::CreateTable(create)] = statements.as_slice() else {
        return Ok(None);
    };

    if create.or_replace {
        return Err(unsupported("CREATE OR REPLACE TABLE"));
    }
    if create.external {
        return Err(unsupported("CREATE EXTERNAL TABLE"));
    }
    if create.columns.is_empty() {
        return Err(Error::new_invalid_input(
            CODE_NO_COLUMNS.clone(),
            "CREATE TABLE needs at least one column".to_string(),
        ));
    }

    Ok(Some(CreateTablePlan {
        stream: stream_id(&create.name)?,
        columns: create.columns.iter().map(to_column).collect::<Result<Vec<_>, Error>>()?,
        if_not_exists: create.if_not_exists,
    }))
}

fn unsupported(what: &str) -> Error {
    Error::new_unsupported(CODE_DDL_UNSUPPORTED.clone(), format!("{what} is not supported"))
}

fn to_column(column: &ColumnDef) -> Result<Column, Error> {
    let required = column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::NotNull));
    Ok(Column::new(
        column.name.value.clone(),
        to_column_type(&column.data_type, &column.name.value)?,
        required,
    ))
}

/// SQL type -> silo column type, for the scalar subset silo can ingest today.
/// Anything else is an explicit error: silently coercing an unsupported type
/// into `TEXT` would quietly corrupt the column for the life of the table.
fn to_column_type(sql_type: &SqlType, column: &str) -> Result<ColumnType, Error> {
    match sql_type {
        SqlType::Boolean | SqlType::Bool => Ok(ColumnType::Bool),
        // silo stores integers and floats 64-bit wide; the narrower SQL spellings
        // are accepted and widened rather than rejected.
        SqlType::SmallInt(_) | SqlType::Int(_) | SqlType::Integer(_) | SqlType::BigInt(_) => Ok(ColumnType::Int64),
        SqlType::Real | SqlType::Float(_) | SqlType::Double(_) | SqlType::DoublePrecision => Ok(ColumnType::Float64),
        SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) => Ok(ColumnType::String),
        other => Err(Error::new_unsupported(
            CODE_UNSUPPORTED_TYPE.clone(),
            format!("column '{column}': type {other} is not supported (use BOOLEAN, INT, BIGINT, REAL, DOUBLE PRECISION, or TEXT)"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns_of(sql: &str) -> Vec<Column> {
        detect(sql).expect("valid ddl").expect("recognized").columns
    }

    #[test]
    fn maps_the_supported_scalar_types() {
        let columns = columns_of("CREATE TABLE demo.events (id BIGINT, name TEXT, score DOUBLE PRECISION, ok BOOLEAN)");
        assert_eq!(
            columns,
            vec![
                Column::new("id", ColumnType::Int64, false),
                Column::new("name", ColumnType::String, false),
                Column::new("score", ColumnType::Float64, false),
                Column::new("ok", ColumnType::Bool, false),
            ]
        );
    }

    #[test]
    fn extracts_the_stream_id() {
        let plan = detect("CREATE TABLE demo.events (id BIGINT)").unwrap().unwrap();
        assert_eq!(plan.stream, StreamId::new(["demo"], "events"));
        assert!(!plan.if_not_exists);
    }

    #[test]
    fn if_not_exists_is_carried_through() {
        let plan = detect("CREATE TABLE IF NOT EXISTS demo.events (id BIGINT)").unwrap().unwrap();
        assert!(plan.if_not_exists);
    }

    #[test]
    fn not_null_makes_the_column_required() {
        let columns = columns_of("CREATE TABLE demo.t (id BIGINT NOT NULL, note TEXT)");
        assert!(columns[0].required);
        assert!(!columns[1].required);
    }

    #[test]
    fn an_unsupported_column_type_is_rejected() {
        assert!(detect("CREATE TABLE demo.t (when TIMESTAMP)").is_err());
    }

    #[test]
    fn a_select_is_not_ddl() {
        assert!(detect("SELECT 1").unwrap().is_none());
    }
}
