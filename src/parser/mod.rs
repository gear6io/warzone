//! SQL parsing foundation.
//!
//! Turns a SQL string into a `sqlparser` AST (`Vec<Statement>`) using the
//! PostgreSQL dialect, and re-exports the visitor surface so consumers walk the
//! AST through one place instead of depending on `sqlparser` paths directly.
//!
//! Nothing on the request path calls this yet — it is reusable infrastructure.
//! Future consumers (statement routing reads→DuckDB / writes→silo, COPY-FROM-STDIN
//! detection, query rewrite/validation) build on `parse` + the visitor traits.

use std::sync::LazyLock;

use errors::{Code, Error};
use silo::StreamId;
use sqlparser::parser::Parser;
use sqlparser::{ast::ObjectName, dialect::PostgreSqlDialect};

// Re-export the AST + visitor surface. Consumers use `parser::ast::…` and the
// visitor traits/helpers without pulling `sqlparser` into their own imports.
pub use sqlparser::ast;
#[allow(unused_imports)]
pub use sqlparser::ast::{
    visit_relations, visit_statements, Statement, Visit, VisitMut, Visitor, VisitorMut,
};

static CODE_INVALID_SQL: LazyLock<Code> = LazyLock::new(|| Code::must_new("invalid_sql"));
static CODE_MISSING_TABLE_NAME: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("missing_table_name"));

/// Parses `sql` as one or more PostgreSQL statements.
///
/// A `sqlparser::ParserError` is mapped to an `InvalidInput`-typed [`Error`] at
/// this boundary — the same type the querier assigns to malformed SQL — so
/// callers branch on `err.is_type(errors::Type::InvalidInput)` uniformly.
pub fn parse(sql: &str) -> Result<Vec<Statement>, Error> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| {
        Error::wrap_invalid_input(e, CODE_INVALID_SQL.clone(), format!("invalid sql: {sql}"))
    })
}

/// `demo.events` -> namespace `["demo"]`, table `events`. Shared by the two
/// statements that name a table: `CREATE TABLE` and `COPY ... FROM STDIN`.
pub fn stream_id(name: &ObjectName) -> Result<StreamId, Error> {
    let mut parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    let table = parts.pop().ok_or_else(|| {
        Error::new_invalid_input(
            CODE_MISSING_TABLE_NAME.clone(),
            "statement has no table name".to_string(),
        )
    })?;
    Ok(StreamId::new(parts, table))
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use super::*;

    #[test]
    fn parses_a_single_select() {
        let statements =
            parse("SELECT id, name FROM demo.events").expect("valid select should parse");
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn visitor_collects_relation_names() {
        let statements =
            parse("SELECT id, name FROM demo.events").expect("valid select should parse");

        // Walk the AST via the visitor surface and collect every table reference.
        let mut names = Vec::new();
        let _: ControlFlow<()> = visit_relations(&statements, |relation| {
            names.push(relation.to_string());
            ControlFlow::Continue(())
        });

        assert_eq!(names, vec!["demo.events".to_string()]);
    }

    #[test]
    fn malformed_sql_maps_to_invalid_input() {
        let err = parse("SELEC 1").expect_err("malformed sql should fail");
        assert!(err.is_type(errors::Type::InvalidInput));
    }
}
