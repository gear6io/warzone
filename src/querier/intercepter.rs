use std::{ops::Index, sync::LazyLock};

use errors::{Code, Error};
use sqlparser::ast::{CreateTable, Statement};

use crate::{parser::stream_id, server::pgwire};

static CODE_FAILED_PARSE_QUERY: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("query_parser_failed"));

static CODE_MULTI_LINE_STMT: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("query_multi_line_stmt"));
static CODE_DDL_UNSUPPORTED: LazyLock<Code> = LazyLock::new(|| Code::must_new("ddl_unsupported"));
static CODE_UNSUPPORTED_STMT: LazyLock<Code> = LazyLock::new(|| Code::must_new("unsupported_stmt"));
static CODE_NO_COLUMNS: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("create_table_no_columns"));

const MAX_STATEMENT_LINES: usize = 1;

pub struct QueryIntercepter {
    query_input: String,
    query_handler: pgwire::Handler,
}

impl QueryIntercepter {
    pub fn new(query: String, handler: pgwire::Handler) -> Self {
        return QueryIntercepter {
            query_input: query,
            query_handler: handler,
        };
    }

    pub fn run(&self) -> Result<String, Error> {
        let statements = crate::parser::parse(&self.query_input)
            .map_err(|err| Error::wrap_invalid_input(err, CODE_FAILED_PARSE_QUERY.clone(), ""))?;

        if statements.len() > MAX_STATEMENT_LINES {
            return Err(Error::new_invalid_input(
                CODE_MULTI_LINE_STMT.clone(),
                "multi line statements are not supported!",
            ));
        }

        match statements.index(0) {
            Statement::CreateTable(create) => &self.handle_create_table(create),
            _ => return Err(Error::new_forbidden(CODE_UNSUPPORTED_STMT.clone(), "")),
        };

        Ok(("".to_string()))
    }

    fn handle_create_table(&self, create: &CreateTable) -> Result<String, Error> {
        if create.or_replace {
            return Err(Self::unsupported("CREATE OR REPLACE TABLE"));
        }
        if create.external {
            return Err(Self::unsupported("CREATE EXTERNAL TABLE"));
        }
        if create.columns.is_empty() {
            return Err(Error::new_invalid_input(
                CODE_NO_COLUMNS.clone(),
                "CREATE TABLE needs at least one column".to_string(),
            ));
        }

        let id = stream_id(&create.name);

        self.query_handler
            .create_table(&id, create.columns, create.if_not_exists)
    }

    fn unsupported(what: &str) -> Error {
        Error::new_unsupported(
            CODE_DDL_UNSUPPORTED.clone(),
            format!("{what} is not supported"),
        )
    }
}
