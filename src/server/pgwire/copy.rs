//! `COPY <ns>.<table> FROM STDIN` ingest over the Postgres wire protocol.
//!
//! One COPY command = one silo ingest session: `begin_write` on the first data
//! row, `write` a `RecordBatch` per arriving `CopyData` chunk, one `commit` at
//! `CopyDone` — the stream closing is what commits the file, nothing else.
//! Rows are never held back waiting for a row-count threshold. This collapses
//! the HTTP one-row-per-request path
//! ([crate::server::http::v1::payload]) — which is one Parquet file + one
//! Iceberg snapshot per row — into one file + one snapshot per COPY, which is
//! the throughput point of routing bulk ingest here.
//!
//! ponytail: column types are inferred from the first data row (CSV/text are
//! untyped on the wire) and then fixed for the whole COPY. This is lossy —
//! `"01234"` becomes `Int64` and loses the leading zero, a first-row NULL
//! pins the column to `Utf8`. Upgrade path: parse fields into the target
//! table's already-known Iceberg column types instead of guessing (needs a
//! schema accessor on `TableSink`). See docs/high-throughput-ingestion.mdx §5.
//!
//! ponytail: CSV field splitting is a plain delimiter split — no quoted-field
//! or embedded-delimiter/newline handling. Add a real CSV reader if that
//! matters.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use tokio::sync::Mutex;

use errors::{Code, Error};
use silo::ingest::schema::to_iceberg_schema;
use silo::ingest::{IcebergTableSink, IngestSession, TableSink};
use silo::StreamId;

use crate::parser::ast::{CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Statement};

fn unsupported(what: String) -> Error {
    Error::new_unsupported(Code::must_new("copy_unsupported"), format!("{what} is not supported"))
}

/// Per-connection COPY state, stashed in the client's session extensions so it
/// survives across `on_copy_data` / `on_copy_done` callbacks. Those extensions
/// are a type-keyed map, so this newtype is what keys the entry — that, not
/// encapsulation, is why it wraps the `Mutex` instead of the handler storing
/// one directly.
pub struct CopyState {
    /// Column count advertised in `CopyInResponse`. Zero when the CSV header
    /// has yet to supply the names — harmless for text-format COPY.
    pub columns: usize,
    pub inner: Mutex<CopyInner>,
}

/// Detects a lone `COPY <table> FROM STDIN`. Returns `Ok(None)` for anything
/// that is not one (including SQL that fails to parse — let the querier report
/// it), and `Err` for a COPY-FROM-STDIN we recognize but cannot serve.
pub fn detect(sql: &str) -> Result<Option<CopyState>, Error> {
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

    // namespace = all identifier parts but the last; table = the last.
    let mut parts: Vec<String> = table_name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect();
    let table = parts.pop().ok_or_else(|| {
        Error::new_invalid_input(Code::must_new("copy_missing_table"), "COPY target has no table name".to_string())
    })?;
    let stream = StreamId::new(parts, table);

    let format = resolve_options(options, legacy_options)?;

    let columns: Vec<String> = columns.iter().map(|ident| ident.value.clone()).collect();
    if columns.is_empty() && !format.header {
        return Err(Error::new_invalid_input(
            Code::must_new("copy_columns_unknown"),
            "COPY needs an explicit column list, e.g. COPY t (a, b) FROM STDIN, or CSV HEADER".to_string(),
        ));
    }

    Ok(Some(CopyState {
        columns: columns.len(),
        inner: Mutex::new(CopyInner {
            stream,
            delimiter: format.delimiter,
            null_marker: format.null_marker,
            header_pending: format.header,
            columns,
            leftover: Vec::new(),
            rows: Vec::new(),
            schema: None,
            session: None,
            total: 0,
        }),
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

pub struct CopyInner {
    stream: StreamId,
    delimiter: char,
    null_marker: String,
    header_pending: bool,
    columns: Vec<String>,
    leftover: Vec<u8>,
    rows: Vec<Vec<Option<String>>>,
    /// Set once, from the first data row — fixes column types for the COPY.
    schema: Option<Arc<ArrowSchema>>,
    session: Option<Box<dyn IngestSession>>,
    total: usize,
}

impl CopyInner {
    /// Feed a chunk of raw COPY bytes: split off every complete line and
    /// process it, keeping any partial trailing line in `leftover`. The chunk's
    /// rows are written straight through — one `RecordBatch` per chunk.
    pub async fn on_data(&mut self, sink: &Arc<Mutex<IcebergTableSink>>, bytes: &[u8]) -> Result<(), Error> {
        self.leftover.extend_from_slice(bytes);
        while let Some(pos) = self.leftover.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.leftover.drain(..=pos).collect();
            self.process_line(sink, &line[..line.len() - 1]).await?;
        }
        self.flush().await
    }

    /// Stream closed: flush the trailing line, commit the file, report the row count.
    pub async fn finish(&mut self, sink: &Arc<Mutex<IcebergTableSink>>) -> Result<usize, Error> {
        if !self.leftover.is_empty() {
            let line = std::mem::take(&mut self.leftover);
            self.process_line(sink, &line).await?;
        }
        self.flush().await?;
        if let Some(session) = self.session.take() {
            session.commit().await?;
        }
        Ok(self.total)
    }

    /// Discard a partial COPY: delete the partial Parquet file, touch no catalog.
    ///
    /// TODO: this only runs on an explicit `CopyFail` message or a parse/write
    /// error. It does NOT run when the client's TCP connection drops mid-COPY
    /// (psql killed, network cut, container evicted). pgwire's
    /// `process_socket` loop just ends on a dead socket — no `CopyFail` frame
    /// is synthesized — so the `CopyState` in the session extensions is
    /// dropped, taking the open `Box<dyn IngestSession>` with it. Nothing in
    /// `crates/silo/` implements `Drop`, so `IngestSession::abort` (which is
    /// what deletes the partial Parquet file) never runs, and the half-written
    /// file is orphaned in the warehouse. No catalog damage — the snapshot is
    /// only committed at `CopyDone` — but the bytes leak, and a long-running
    /// server accumulates them.
    ///
    /// Two possible fixes, neither done here:
    ///  1. `impl Drop for IcebergIngestSession` — hard: `abort()` is async and
    ///     takes `Box<Self>`, so it needs a `tokio::spawn` of the delete on a
    ///     handle captured at construction, and a flag so an explicit
    ///     commit/abort doesn't double-fire.
    ///  2. Sweep orphans out-of-band: any data file under the table's data dir
    ///     not referenced by a snapshot, older than some COPY-duration bound,
    ///     is garbage. Fits with the background compaction that
    ///     `docs/high-throughput-ingestion.mdx` already lists as planned.
    ///
    /// Also fix `docs/high-throughput-ingestion.mdx` §3 (claims "connection
    /// drop → session.abort()") and its verification step 6 (asserts no
    /// partial Parquet file after killing psql mid-COPY) — both are wrong
    /// until one of the above lands.
    pub async fn abort(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = session.abort().await;
        }
    }

    async fn process_line(&mut self, sink: &Arc<Mutex<IcebergTableSink>>, line: &[u8]) -> Result<(), Error> {
        // An empty line is not "nothing": in text format it is a one-column row
        // holding the empty string. Feeding it through the field split below
        // either accepts it as that row or trips the column-count check — both
        // beat dropping it and reporting a row count that does not match what
        // the client sent.
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let text = std::str::from_utf8(line).map_err(|e| {
            Error::wrap_invalid_input(e, Code::must_new("copy_invalid_utf8"), "COPY data is not valid UTF-8")
        })?;

        if self.header_pending {
            if self.columns.is_empty() {
                self.columns = text.split(self.delimiter).map(|f| f.to_string()).collect();
            }
            self.header_pending = false;
            return Ok(());
        }

        let fields: Vec<Option<String>> = text
            .split(self.delimiter)
            .map(|f| if f == self.null_marker { None } else { Some(f.to_string()) })
            .collect();
        if fields.len() != self.columns.len() {
            return Err(Error::new_invalid_input(
                Code::must_new("copy_column_count_mismatch"),
                format!("COPY row has {} fields but {} columns were declared", fields.len(), self.columns.len()),
            ));
        }

        if self.schema.is_none() {
            self.setup(sink, &fields).await?;
        }
        self.rows.push(fields);
        self.total += 1;
        Ok(())
    }

    /// First data row: infer + fix column types, ensure the table, open a session.
    async fn setup(&mut self, sink: &Arc<Mutex<IcebergTableSink>>, first_row: &[Option<String>]) -> Result<(), Error> {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .zip(first_row)
            .map(|(name, value)| {
                let data_type = value.as_deref().map(infer_type).unwrap_or(DataType::Utf8);
                Field::new(name, data_type, true)
            })
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        let iceberg_schema = to_iceberg_schema(schema.as_ref())?;

        let mut guard = sink.lock().await;
        guard.setup(&self.stream, &iceberg_schema).await?;
        let session = guard.begin_write(&self.stream).await?;
        drop(guard);

        self.schema = Some(schema);
        self.session = Some(session);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let schema = self.schema.clone().expect("schema is set before any row is buffered");
        let rows = std::mem::take(&mut self.rows);
        let batch = build_batch(&schema, &rows)?;
        self.session
            .as_mut()
            .expect("session is opened before any row is buffered")
            .write(batch)
            .await
    }
}

/// Infers an Arrow type from one wire value: integer, then float, then boolean,
/// else text. ponytail: order matters and it is a guess — see the module note.
fn infer_type(value: &str) -> DataType {
    if value.parse::<i64>().is_ok() {
        DataType::Int64
    } else if value.parse::<f64>().is_ok() {
        DataType::Float64
    } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        DataType::Boolean
    } else {
        DataType::Utf8
    }
}

fn build_batch(schema: &Arc<ArrowSchema>, rows: &[Vec<Option<String>>]) -> Result<RecordBatch, Error> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let column: Arc<dyn Array> = match field.data_type() {
            DataType::Int64 => Arc::new(
                rows.iter()
                    .map(|row| row[idx].as_deref().map(parse_i64).transpose())
                    .collect::<Result<Int64Array, _>>()?,
            ),
            DataType::Float64 => Arc::new(
                rows.iter()
                    .map(|row| row[idx].as_deref().map(parse_f64).transpose())
                    .collect::<Result<Float64Array, _>>()?,
            ),
            DataType::Boolean => Arc::new(
                rows.iter()
                    .map(|row| row[idx].as_deref().map(parse_bool).transpose())
                    .collect::<Result<BooleanArray, _>>()?,
            ),
            _ => Arc::new(rows.iter().map(|row| row[idx].clone()).collect::<StringArray>()),
        };
        columns.push(column);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        Error::wrap_invalid_input(e, Code::must_new("copy_batch_build_failed"), "failed to build record batch from COPY rows")
    })
}

fn parse_i64(value: &str) -> Result<i64, Error> {
    value.parse::<i64>().map_err(|e| {
        Error::wrap_invalid_input(e, Code::must_new("copy_parse_int_failed"), format!("cannot parse '{value}' as integer"))
    })
}

fn parse_f64(value: &str) -> Result<f64, Error> {
    value.parse::<f64>().map_err(|e| {
        Error::wrap_invalid_input(e, Code::must_new("copy_parse_float_failed"), format!("cannot parse '{value}' as float"))
    })
}

fn parse_bool(value: &str) -> Result<bool, Error> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Ok(true),
        "false" | "f" | "0" => Ok(false),
        _ => Err(Error::new_invalid_input(
            Code::must_new("copy_parse_bool_failed"),
            format!("cannot parse '{value}' as boolean"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_of(sql: &str) -> CopyInner {
        detect(sql).expect("valid copy").expect("recognized as copy").inner.into_inner()
    }

    #[test]
    fn detect_copy_from_stdin_extracts_target_and_columns() {
        let plan = plan_of("COPY demo.events (id, name) FROM STDIN WITH (FORMAT csv)");
        assert_eq!(plan.stream, StreamId::new(["demo"], "events"));
        assert_eq!(plan.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(plan.delimiter, ',');
        assert_eq!(plan.null_marker, "");
    }

    #[test]
    fn text_format_is_the_default() {
        let plan = plan_of("COPY demo.events (id) FROM STDIN");
        assert_eq!(plan.delimiter, '\t');
        assert_eq!(plan.null_marker, "\\N");
    }

    /// psql's `\copy t from 'f' csv header` emits pre-9.0 syntax, which
    /// `sqlparser` reports in `legacy_options`, not `options`. Honouring only
    /// the modern list silently ingests these with the wrong delimiter.
    #[test]
    fn legacy_options_are_honoured() {
        let plan = plan_of("COPY demo.events (id, name) FROM STDIN CSV HEADER");
        assert_eq!(plan.delimiter, ',');
        assert!(plan.header_pending);

        let plan = plan_of("COPY demo.events (id, name) FROM STDIN DELIMITER '|'");
        assert_eq!(plan.delimiter, '|');
    }

    #[test]
    fn options_we_cannot_honour_are_rejected() {
        // Both spellings of an option whose semantics we do not implement.
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

    #[test]
    fn copy_without_columns_or_header_is_rejected() {
        assert!(detect("COPY demo.events FROM STDIN").is_err());
    }

    #[test]
    fn build_batch_parses_inferred_types() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let rows = vec![
            vec![Some("1".to_string()), Some("a".to_string())],
            vec![Some("2".to_string()), None],
        ];
        let batch = build_batch(&schema, &rows).expect("builds");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.column(1).is_null(1));
    }

    #[test]
    fn build_batch_rejects_unparseable_int() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int64, true)]));
        let rows = vec![vec![Some("not-a-number".to_string())]];
        assert!(build_batch(&schema, &rows).is_err());
    }
}
