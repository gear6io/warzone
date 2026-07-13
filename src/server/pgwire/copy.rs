//! `COPY <ns>.<table> FROM STDIN` ingest over the Postgres wire protocol.
//!
//! One COPY command = one silo ingest session: `begin_write` when the first row
//! arrives, `push` per row, one `commit` at `CopyDone` — the stream closing is
//! what commits the file, nothing else.
//!
//! **This module holds no rows and knows nothing about Arrow.** It parses wire
//! bytes into `silo::ingest::Value`s against the table's *registered* column
//! types and pushes them one at a time; how many rows get gathered into a
//! columnar block before they reach Parquet is silo's business
//! ([`silo::ingest::IngestSession::push`]). The only state that crosses a wire
//! chunk here is `leftover` — at most one partial *line*, because a chunk can
//! split mid-row.
//!
//! Types come from `CREATE TABLE` (see [`super::ddl`]), never from the data.
//! The old code inferred them from the first row, which silently turned a CSV
//! `01234` into the integer `1234`; against a registered `TEXT` column it now
//! stays `"01234"`.
//!
//! ponytail: CSV field splitting is a plain delimiter split — no quoted-field
//! or embedded-delimiter/newline handling. Add a real CSV reader if that matters.

use std::sync::{Arc, LazyLock};

use tokio::sync::Mutex;

use errors::{Code, Error};
use silo::ingest::{Column, ColumnType, IcebergTableSink, IngestSession, TableSink, Value};
use silo::StreamId;

use crate::parser::ast::{CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Statement};

use super::stream_id;

static CODE_COPY_UNSUPPORTED: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_unsupported"));
static CODE_UNKNOWN_COLUMN: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_unknown_column"));
static CODE_INVALID_UTF8: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_invalid_utf8"));
static CODE_COLUMN_COUNT_MISMATCH: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_column_count_mismatch"));
static CODE_PARSE_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_parse_failed"));

fn unsupported(what: String) -> Error {
    Error::new_unsupported(CODE_COPY_UNSUPPORTED.clone(), format!("{what} is not supported"))
}

/// What a `COPY ... FROM STDIN` statement asks for, before the table's schema is
/// known. [`CopyState::new`] resolves it against the registered schema.
pub struct CopyPlan {
    pub stream: StreamId,
    /// The explicit column list, if the statement carried one. Empty means "the
    /// table's columns" — supplied either by a CSV header or, failing that, by
    /// the table's own column order.
    columns: Vec<String>,
    format: CopyFormat,
}

/// Detects a lone `COPY <table> FROM STDIN`. Returns `Ok(None)` for anything
/// that is not one (including SQL that fails to parse — let the querier report
/// it), and `Err` for a COPY-FROM-STDIN we recognize but cannot serve.
pub fn detect(sql: &str) -> Result<Option<CopyPlan>, Error> {
    let statements = match crate::parser::parse(sql) {
        Ok(statements) => statements,
        Err(_) => return Ok(None),
    };

    // Only a statement on its own can become a COPY: entering COPY-IN mode for
    // one statement of a batch would discard its siblings, which never run.
    let [statement] = statements.as_slice() else {
        return if statements.iter().any(is_copy_from_stdin) {
            Err(unsupported("COPY FROM STDIN alongside other statements in one query".to_string()))
        } else {
            Ok(None)
        };
    };

    let Statement::Copy {
        source: CopySource::Table { table_name, columns },
        to: false,
        target: CopyTarget::Stdin,
        options,
        legacy_options,
        values,
    } = statement
    else {
        return Ok(None);
    };
    if !values.is_empty() {
        return Err(unsupported("COPY FROM STDIN with inline values".to_string()));
    }

    Ok(Some(CopyPlan {
        stream: stream_id(table_name)?,
        columns: columns.iter().map(|ident| ident.value.clone()).collect(),
        format: resolve_options(options, legacy_options)?,
    }))
}

fn is_copy_from_stdin(statement: &Statement) -> bool {
    matches!(statement, Statement::Copy { to: false, target: CopyTarget::Stdin, .. })
}

/// The wire settings a COPY resolves to.
struct CopyFormat {
    delimiter: char,
    null_marker: String,
    /// A header line is present and must be consumed before data.
    header: bool,
}

/// Folds the two option lists `sqlparser` produces — modern `WITH (...)` and
/// the pre-9.0 legacy syntax psql still emits (`COPY t FROM STDIN CSV HEADER`)
/// — into the settings we honour. The format picks the delimiter/null defaults;
/// explicit options override. Anything we cannot honour is an error, never a
/// silent default: an ignored `QUOTE` or `DELIMITER` parses the rows wrong and
/// says nothing.
fn resolve_options(options: &[CopyOption], legacy_options: &[CopyLegacyOption]) -> Result<CopyFormat, Error> {
    let mut csv = false;
    let mut delimiter: Option<char> = None;
    let mut null_marker: Option<String> = None;
    let mut header = false;

    for option in options {
        match option {
            CopyOption::Format(ident) => match ident.value.to_ascii_lowercase().as_str() {
                "csv" => csv = true,
                "text" => csv = false,
                other => return Err(unsupported(format!("COPY FORMAT '{other}' (use text or csv)"))),
            },
            CopyOption::Delimiter(ch) => delimiter = Some(*ch),
            CopyOption::Null(marker) => null_marker = Some(marker.clone()),
            CopyOption::Header(flag) => header = *flag,
            other => return Err(unsupported(format!("COPY option {other}"))),
        }
    }

    for option in legacy_options {
        match option {
            CopyLegacyOption::Csv(csv_options) => {
                csv = true;
                for csv_option in csv_options {
                    match csv_option {
                        CopyLegacyCsvOption::Header => header = true,
                        other => return Err(unsupported(format!("COPY option {other}"))),
                    }
                }
            }
            CopyLegacyOption::Delimiter(ch) => delimiter = Some(*ch),
            CopyLegacyOption::Null(marker) => null_marker = Some(marker.clone()),
            CopyLegacyOption::Header => header = true,
            other => return Err(unsupported(format!("COPY option {other}"))),
        }
    }

    let (default_delim, default_null) = if csv { (',', String::new()) } else { ('\t', "\\N".to_string()) };
    Ok(CopyFormat {
        delimiter: delimiter.unwrap_or(default_delim),
        null_marker: null_marker.unwrap_or(default_null),
        header,
    })
}

/// Per-connection COPY state, stashed in the client's session extensions so it
/// survives across `on_copy_data` / `on_copy_done` callbacks. Those extensions
/// are a type-keyed map, so this newtype is what keys the entry.
pub struct CopyState {
    /// Column count advertised in `CopyInResponse`.
    pub columns: usize,
    pub inner: Mutex<CopyInner>,
}

impl CopyState {
    /// Resolve a parsed COPY against the table's registered schema: work out
    /// which column of the table each wire field lands in.
    pub fn new(plan: CopyPlan, schema: &[Column]) -> Result<Self, Error> {
        // No explicit column list and no header to supply one: the wire fields
        // are the table's columns, in the table's order (Postgres' default).
        let targets = if plan.columns.is_empty() && !plan.format.header {
            Some((0..schema.len()).collect())
        } else if plan.columns.is_empty() {
            None // the CSV header names them; resolved when that line arrives
        } else {
            Some(resolve_targets(&plan.columns, schema)?)
        };

        Ok(Self {
            columns: targets.as_ref().map_or(0, Vec::len),
            inner: Mutex::new(CopyInner {
                stream: plan.stream,
                delimiter: plan.format.delimiter,
                null_marker: plan.format.null_marker,
                header_pending: plan.format.header,
                schema: schema.to_vec(),
                targets,
                leftover: Vec::new(),
                session: None,
                total: 0,
            }),
        })
    }
}

/// Map each named column to its position in the table. An unknown column name is
/// an error — Postgres would reject it, and silently dropping the field would
/// shift every subsequent one into the wrong column.
fn resolve_targets(names: &[String], schema: &[Column]) -> Result<Vec<usize>, Error> {
    names
        .iter()
        .map(|name| {
            schema.iter().position(|column| &column.name == name).ok_or_else(|| {
                Error::new_invalid_input(
                    CODE_UNKNOWN_COLUMN.clone(),
                    format!("column '{name}' does not exist on this table"),
                )
            })
        })
        .collect()
}

pub struct CopyInner {
    stream: StreamId,
    delimiter: char,
    null_marker: String,
    header_pending: bool,
    /// The table's columns. A pushed row is always this wide, with columns the
    /// COPY did not mention left `Null`.
    schema: Vec<Column>,
    /// Which column of `schema` each wire field fills, in wire order. `None` only
    /// while a CSV header is still owed; the header names the columns.
    targets: Option<Vec<usize>>,
    /// The one thing that crosses a chunk boundary: at most one partial line.
    leftover: Vec<u8>,
    session: Option<Box<dyn IngestSession>>,
    total: usize,
}

impl CopyInner {
    /// Feed a chunk of raw COPY bytes: push every complete line it contains,
    /// keeping only a partial trailing line for the next chunk.
    pub async fn on_data(&mut self, sink: &Arc<Mutex<IcebergTableSink>>, bytes: &[u8]) -> Result<(), Error> {
        self.leftover.extend_from_slice(bytes);
        while let Some(pos) = self.leftover.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.leftover.drain(..=pos).collect();
            self.process_line(sink, &line[..line.len() - 1]).await?;
        }
        Ok(())
    }

    /// Stream closed: push the trailing line, commit the file, report the count.
    pub async fn finish(&mut self, sink: &Arc<Mutex<IcebergTableSink>>) -> Result<usize, Error> {
        if !self.leftover.is_empty() {
            let line = std::mem::take(&mut self.leftover);
            self.process_line(sink, &line).await?;
        }
        if let Some(session) = self.session.take() {
            session.commit().await?;
        }
        Ok(self.total)
    }

    /// Discard a partial COPY: delete the partial Parquet file, touch no catalog.
    ///
    /// TODO: this only runs on an explicit `CopyFail` message or a parse/write
    /// error. It does NOT run when the client's TCP connection drops mid-COPY
    /// (psql killed, network cut, container evicted). pgwire's `process_socket`
    /// loop just ends on a dead socket — no `CopyFail` frame is synthesized — so
    /// the `CopyState` is dropped, taking the open `Box<dyn IngestSession>` with
    /// it. Nothing in `crates/silo/` implements `Drop`, so `IngestSession::abort`
    /// (which deletes the partial Parquet file) never runs and the half-written
    /// file is orphaned. No catalog damage — the snapshot is only committed at
    /// `CopyDone` — but the bytes leak, and a long-running server accumulates
    /// them. Fix by either implementing `Drop` (awkward: `abort` is async and
    /// takes `Box<Self>`) or sweeping unreferenced data files out-of-band, which
    /// fits with the background compaction already planned.
    pub async fn abort(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = session.abort().await;
        }
    }

    async fn process_line(&mut self, sink: &Arc<Mutex<IcebergTableSink>>, line: &[u8]) -> Result<(), Error> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let text = std::str::from_utf8(line).map_err(|e| {
            Error::wrap_invalid_input(e, CODE_INVALID_UTF8.clone(), "COPY data is not valid UTF-8")
        })?;

        if self.header_pending {
            self.header_pending = false;
            if self.targets.is_none() {
                let names: Vec<String> = text.split(self.delimiter).map(str::to_string).collect();
                self.targets = Some(resolve_targets(&names, &self.schema)?);
            }
            return Ok(());
        }

        let targets = self.targets.as_deref().expect("targets are resolved before any data line");
        let fields: Vec<&str> = text.split(self.delimiter).collect();
        if fields.len() != targets.len() {
            return Err(Error::new_invalid_input(
                CODE_COLUMN_COUNT_MISMATCH.clone(),
                format!("COPY row has {} fields but {} columns were declared", fields.len(), targets.len()),
            ));
        }

        // Columns the COPY never mentions stay Null — that is what Postgres does,
        // and silo will reject it if the column is actually required.
        let mut row = vec![Value::Null; self.schema.len()];
        for (&index, field) in targets.iter().zip(fields) {
            if field != self.null_marker {
                row[index] = parse_value(field, &self.schema[index])?;
            }
        }

        if self.session.is_none() {
            self.session = Some(sink.lock().await.begin_write(&self.stream).await?);
        }
        self.session
            .as_mut()
            .expect("session opened just above")
            .push(row)
            .await?;
        self.total += 1;
        Ok(())
    }
}

/// One wire field -> one `Value`, parsed as the column's *registered* type. No
/// inference: the type was fixed at `CREATE TABLE`.
fn parse_value(text: &str, column: &Column) -> Result<Value, Error> {
    let invalid = |what: &str| {
        Error::new_invalid_input(
            CODE_PARSE_FAILED.clone(),
            format!("column '{}': cannot parse '{text}' as {what}", column.name),
        )
    };
    match column.column_type {
        ColumnType::Bool => match text.to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => Ok(Value::Bool(true)),
            "false" | "f" | "no" | "n" | "0" => Ok(Value::Bool(false)),
            _ => Err(invalid("a boolean")),
        },
        ColumnType::Int64 => text.parse::<i64>().map(Value::Int64).map_err(|_| invalid("an integer")),
        ColumnType::Float64 => text.parse::<f64>().map(Value::Float64).map_err(|_| invalid("a number")),
        ColumnType::String => Ok(Value::String(text.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_of(sql: &str) -> CopyPlan {
        detect(sql).expect("valid copy").expect("recognized as copy")
    }

    /// id BIGINT, name TEXT, score DOUBLE PRECISION
    fn schema() -> Vec<Column> {
        vec![
            Column::new("id", ColumnType::Int64, false),
            Column::new("name", ColumnType::String, false),
            Column::new("score", ColumnType::Float64, false),
        ]
    }

    #[test]
    fn detect_copy_from_stdin_extracts_target_and_columns() {
        let plan = plan_of("COPY demo.events (id, name) FROM STDIN WITH (FORMAT csv)");
        assert_eq!(plan.stream, StreamId::new(["demo"], "events"));
        assert_eq!(plan.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(plan.format.delimiter, ',');
        assert_eq!(plan.format.null_marker, "");
    }

    #[test]
    fn text_format_is_the_default() {
        let plan = plan_of("COPY demo.events (id) FROM STDIN");
        assert_eq!(plan.format.delimiter, '\t');
        assert_eq!(plan.format.null_marker, "\\N");
    }

    /// psql's `\copy t from 'f' csv header` emits pre-9.0 syntax, which
    /// `sqlparser` reports in `legacy_options`, not `options`. Honouring only
    /// the modern list silently ingests these with the wrong delimiter.
    #[test]
    fn legacy_options_are_honoured() {
        let plan = plan_of("COPY demo.events (id, name) FROM STDIN CSV HEADER");
        assert_eq!(plan.format.delimiter, ',');
        assert!(plan.format.header);

        let plan = plan_of("COPY demo.events (id, name) FROM STDIN DELIMITER '|'");
        assert_eq!(plan.format.delimiter, '|');
    }

    #[test]
    fn options_we_cannot_honour_are_rejected() {
        assert!(detect("COPY demo.events (id) FROM STDIN WITH (FORMAT csv, QUOTE '~')").is_err());
        assert!(detect("COPY demo.events (id) FROM STDIN CSV QUOTE '~'").is_err());
        assert!(detect("COPY demo.events (id) FROM STDIN BINARY").is_err());
        assert!(detect("COPY demo.events (id) FROM STDIN WITH (FORMAT parquet)").is_err());
    }

    #[test]
    fn select_is_not_a_copy() {
        assert!(detect("SELECT 1").unwrap().is_none());
    }

    /// Entering COPY-IN mode here would drop the sibling statement on the floor.
    #[test]
    fn copy_in_a_multi_statement_batch_is_rejected() {
        assert!(detect("SELECT 1; COPY demo.events (id) FROM STDIN").is_err());
        assert!(detect("SELECT 1; SELECT 2").unwrap().is_none());
    }

    /// No column list and no header: the wire fields are the table's columns.
    #[test]
    fn bare_copy_targets_every_column_in_table_order() {
        let state = CopyState::new(plan_of("COPY demo.events FROM STDIN"), &schema()).unwrap();
        assert_eq!(state.columns, 3);
    }

    #[test]
    fn a_column_the_table_does_not_have_is_rejected() {
        let plan = plan_of("COPY demo.events (id, nope) FROM STDIN");
        assert!(CopyState::new(plan, &schema()).is_err());
    }

    /// A subset column list still produces a full-width row; the rest are Null.
    #[test]
    fn subset_and_reordered_columns_map_to_the_right_positions() {
        let plan = plan_of("COPY demo.events (score, id) FROM STDIN");
        let state = CopyState::new(plan, &schema()).unwrap();
        let inner = state.inner.into_inner();
        assert_eq!(inner.schema.len(), 3);
        assert_eq!(inner.targets.unwrap(), vec![2, 0]); // score is column 2, id is column 0
    }

    #[test]
    fn values_parse_as_the_registered_type_not_an_inferred_one() {
        let text = Column::new("zip", ColumnType::String, false);
        let long = Column::new("id", ColumnType::Int64, false);

        // The leading-zero bug: inference made this the integer 1234. A registered
        // TEXT column keeps the string intact.
        assert_eq!(parse_value("01234", &text).unwrap(), Value::String("01234".into()));
        assert_eq!(parse_value("42", &long).unwrap(), Value::Int64(42));
        assert!(parse_value("not-a-number", &long).is_err());
    }
}
