//! The statement visitor: parse once, then do the job in the match arm.
//!
//! Every SQL string that reaches warzone — over the Postgres wire protocol, over
//! HTTP, over whatever comes next — is walked by an [`Intercepter`]. It holds the
//! machinery it needs (the silo sink, the DuckDB querier) and acts directly:
//! there is no plan object built in one place and read in another.
//!
//! Two statements are *intercepted*, meaning warzone serves them itself:
//!
//! - `CREATE TABLE` registers a schema with silo. This is the *only* way an
//!   Iceberg table comes into being; ingest never creates one, so a column type is
//!   never guessed from whatever row happened to arrive first.
//! - `COPY <table> FROM STDIN` ingests against an already-registered schema.
//!
//! Everything else — reads, other DDL, SQL sqlparser cannot even parse — is handed
//! to the querier untouched.
//!
//! # Why a COPY comes back out
//!
//! `COPY` is not one call. [`Intercepter::visit`] returns "enter COPY-IN mode" and
//! *returns*; the rows then arrive later, as a series of byte-chunk callbacks, and the
//! ingest session must stay open across all of them. So the in-flight COPY is handed to
//! the caller as [`Outcome::CopyIn`] and handed back to [`Intercepter::on_data`] /
//! [`Intercepter::finish`] / [`Intercepter::abort`]. The transport holds it for the life
//! of the connection; the Intercepter itself stays stateless and shared.

use std::sync::{Arc, LazyLock};

use tokio::sync::Mutex;

use errors::{Code, Error};
use silo::ingest::{Column, ColumnType, IngestSession, TableSink, Value};
use silo::StreamId;

use crate::parser::ast::{
    ColumnDef, ColumnOption, CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource,
    CopyTarget, CreateTable, DataType as SqlType, Ident, ObjectName, Statement,
};
use crate::parser::{parse, stream_id};
use crate::querier::{QueryEngine, QueryResult};
use crate::silo::AppState;

static CODE_NO_COLUMNS: LazyLock<Code> = LazyLock::new(|| Code::must_new("create_table_no_columns"));
static CODE_DDL_UNSUPPORTED: LazyLock<Code> = LazyLock::new(|| Code::must_new("ddl_unsupported"));
static CODE_UNSUPPORTED_TYPE: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("create_table_unsupported_type"));
static CODE_COPY_UNSUPPORTED: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_unsupported"));
static CODE_COPY_UNKNOWN_TABLE: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("copy_unknown_table"));
static CODE_UNKNOWN_COLUMN: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_unknown_column"));
static CODE_INVALID_UTF8: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_invalid_utf8"));
static CODE_COLUMN_COUNT_MISMATCH: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("copy_column_count_mismatch"));
static CODE_PARSE_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("copy_parse_failed"));
static CODE_INTERCEPTED_IN_BATCH: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("intercepted_statement_in_batch"));

/// What the [`Intercepter`] did. The transport renders it on its own wire — pgwire
/// builds `Vec<Response>`, HTTP builds a JSON body, and neither needs to know the
/// other exists.
///
/// A transport that cannot serve one of these errors rather than no-op silently: HTTP
/// does exactly that for [`Outcome::CopyIn`], since `COPY ... FROM STDIN` has no
/// request/response equivalent. Dropping the [`CopyInFlight`] leaks nothing — no
/// ingest session is open until the first row.
pub enum Outcome {
    /// A `CREATE TABLE` was registered with silo.
    Created,
    /// Rows came back from the querier.
    Rows(QueryResult),
    /// A COPY is resolved and taking rows. The transport keeps it for the life of the
    /// connection and hands it back to [`Intercepter::on_data`] / [`Intercepter::finish`]
    /// / [`Intercepter::abort`] — it never looks inside.
    CopyIn(CopyInFlight),
}

/// The visitor. Holds the machinery it does its job with and nothing else — no COPY
/// state, so it is `Send + Sync`, takes `&self` everywhere, and is built once and
/// shared rather than per-connection.
///
/// The sink is held as `dyn TableSink` — the trait silo already defines — so this
/// walks against any sink, including a fake one in tests.
pub struct Intercepter {
    sink: Arc<Mutex<dyn TableSink>>,
    querier: Arc<QueryEngine>,
}

/// A `COPY ... FROM STDIN` that has been resolved against its table's registered
/// schema and is now taking rows.
///
/// Owning one *is* "a COPY is in progress" — there is no separate flag to keep in
/// step, and no way to be half-armed. Every [`Intercepter`] method that touches one
/// consumes it, handing it back only if it is still alive, so a COPY that failed or
/// finished cannot be used again and cannot be left behind holding a stale delimiter.
pub struct CopyInFlight {
    stream: StreamId,
    /// The table's registered columns. A pushed row is always this wide, with columns
    /// the COPY did not mention left `Null`.
    schema: Vec<Column>,
    /// Which column of `schema` each wire field fills, in wire order. `None` only
    /// while a CSV header is still owed — the header names the columns.
    targets: Option<Vec<usize>>,
    delimiter: char,
    null_marker: String,
    header_pending: bool,
    /// The one thing that crosses a chunk boundary: at most one partial line.
    leftover: Vec<u8>,
    session: Option<Box<dyn IngestSession>>,
    total: usize,
}

impl CopyInFlight {
    /// Fields per row, for the transport's COPY-IN handshake. Zero while a CSV header
    /// is still owed — the header is what names the columns.
    pub fn width(&self) -> usize {
        self.targets.as_ref().map_or(0, Vec::len)
    }
}

impl Intercepter {
    pub fn new(state: &AppState) -> Self {
        Self {
            sink: state.sink.clone(),
            querier: state.querier.clone(),
        }
    }

    /// Parse `sql` once, then serve it.
    pub async fn visit(&self, sql: &str) -> Result<Outcome, Error> {
        // A parse failure is not an error here. sqlparser's PostgreSQL dialect is
        // strictly narrower than DuckDB's, so `read_parquet(...)`, `PIVOT` and
        // friends land in this branch and are perfectly valid downstream. Let DuckDB
        // have its say — if the SQL is genuinely malformed, DuckDB reports it.
        let Ok(statements) = parse(sql) else {
            return self.passthrough(sql).await;
        };

        match statements.as_slice() {
            [Statement::CreateTable(create)] => self.create_table(create).await,

            [Statement::Copy {
                source: CopySource::Table { table_name, columns },
                to: false,
                target: CopyTarget::Stdin,
                options,
                legacy_options,
                values,
            }] => {
                if !values.is_empty() {
                    return Err(unsupported(
                        &CODE_COPY_UNSUPPORTED,
                        "COPY FROM STDIN with inline values",
                    ));
                }
                self.copy_in(table_name, columns, options, legacy_options).await
            }

            // A batch. An intercepted statement cannot ride along in one: serving it
            // would discard its siblings (COPY takes the connection into COPY-IN mode
            // and they never run), and passing it through would hand a CREATE TABLE to
            // DuckDB, which builds a DuckDB-only table and registers nothing with silo.
            // Either way the input is silently lost, so say so instead.
            statements => {
                if statements.iter().any(is_intercepted) {
                    return Err(Error::new_unsupported(
                        CODE_INTERCEPTED_IN_BATCH.clone(),
                        "CREATE TABLE and COPY FROM STDIN must be sent on their own, not alongside other statements",
                    ));
                }
                self.passthrough(sql).await
            }
        }
    }

    async fn passthrough(&self, sql: &str) -> Result<Outcome, Error> {
        Ok(Outcome::Rows(self.querier.query(sql).await?))
    }

    /// Validate the statement, lower its columns, and register the table with silo.
    ///
    /// No `ALTER TABLE` and no `CREATE OR REPLACE`: iceberg-rust 0.9.1 exposes no way
    /// to evolve an existing table's schema, so we error rather than pretend.
    async fn create_table(&self, create: &CreateTable) -> Result<Outcome, Error> {
        if create.or_replace {
            return Err(unsupported(&CODE_DDL_UNSUPPORTED, "CREATE OR REPLACE TABLE"));
        }
        if create.external {
            return Err(unsupported(&CODE_DDL_UNSUPPORTED, "CREATE EXTERNAL TABLE"));
        }
        if create.columns.is_empty() {
            return Err(Error::new_invalid_input(
                CODE_NO_COLUMNS.clone(),
                "CREATE TABLE needs at least one column".to_string(),
            ));
        }

        let stream = stream_id(&create.name)?;
        let columns = create.columns.iter().map(to_column).collect::<Result<Vec<_>, Error>>()?;

        let mut sink = self.sink.lock().await;
        // `register` errors on a duplicate by itself; the lookup only exists so
        // `IF NOT EXISTS` can turn that into a no-op.
        let already_registered = sink.schema(&stream).await?.is_some();
        if !(create.if_not_exists && already_registered) {
            sink.register(&stream, &columns).await?;
        }
        drop(sink);

        Ok(Outcome::Created)
    }

    /// Resolve a `COPY ... FROM STDIN` against the table's *registered* schema and hand
    /// the result back to the transport, which owns it until the stream closes. An
    /// unregistered table is an error — ingest never creates one.
    async fn copy_in(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        options: &[CopyOption],
        legacy_options: &[CopyLegacyOption],
    ) -> Result<Outcome, Error> {
        let stream = stream_id(table_name)?;
        let schema = self.sink.lock().await.schema(&stream).await?.ok_or_else(|| {
            Error::new_not_found(
                CODE_COPY_UNKNOWN_TABLE.clone(),
                format!("table {stream} does not exist — CREATE TABLE it first"),
            )
        })?;

        let (delimiter, null_marker, header) = copy_format(options, legacy_options)?;

        // No explicit column list and no header to supply one: the wire fields are the
        // table's columns, in the table's order (Postgres' default).
        let targets = if !columns.is_empty() {
            let named: Vec<String> = columns.iter().map(|ident| ident.value.clone()).collect();
            Some(resolve_targets(&named, &schema)?)
        } else if header {
            None // the CSV header names them; resolved when that line arrives
        } else {
            Some((0..schema.len()).collect())
        };

        Ok(Outcome::CopyIn(CopyInFlight {
            stream,
            schema,
            targets,
            delimiter,
            null_marker,
            header_pending: header,
            leftover: Vec::new(),
            session: None,
            total: 0,
        }))
    }

    /// Feed a chunk of raw COPY bytes: push every complete line it contains, keeping
    /// only a partial trailing line for the next chunk.
    ///
    /// The COPY is handed back only if it survives. On failure it is aborted here and
    /// consumed, so the transport cannot go on feeding a dead COPY and cannot forget to
    /// clean one up.
    pub async fn on_data(&self, mut copy: CopyInFlight, bytes: &[u8]) -> Result<CopyInFlight, Error> {
        match self.feed(&mut copy, bytes).await {
            Ok(()) => Ok(copy),
            Err(e) => {
                self.abort(copy).await;
                Err(e)
            }
        }
    }

    async fn feed(&self, copy: &mut CopyInFlight, bytes: &[u8]) -> Result<(), Error> {
        copy.leftover.extend_from_slice(bytes);
        while let Some(pos) = copy.leftover.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = copy.leftover.drain(..=pos).collect();
            self.push_line(copy, &line[..line.len() - 1]).await?;
        }
        Ok(())
    }

    /// Stream closed: push the trailing line, commit the file, report the count. The
    /// stream closing is what commits — nothing else does.
    ///
    /// Consumes the COPY either way: on failure it is aborted here rather than handed
    /// back half-committed.
    pub async fn finish(&self, mut copy: CopyInFlight) -> Result<usize, Error> {
        match self.commit(&mut copy).await {
            Ok(total) => Ok(total),
            Err(e) => {
                self.abort(copy).await;
                Err(e)
            }
        }
    }

    async fn commit(&self, copy: &mut CopyInFlight) -> Result<usize, Error> {
        if !copy.leftover.is_empty() {
            let line = std::mem::take(&mut copy.leftover);
            self.push_line(copy, &line).await?;
        }
        if let Some(session) = copy.session.take() {
            session.commit().await?;
        }
        Ok(copy.total)
    }

    /// Discard a partial COPY: delete the partial Parquet file, touch no catalog.
    ///
    /// TODO: this only runs on an explicit `CopyFail` message or a parse/write error.
    /// It does NOT run when the client's TCP connection drops mid-COPY (psql killed,
    /// network cut, container evicted). pgwire's `process_socket` loop just ends on a
    /// dead socket — no `CopyFail` frame is synthesized — so the session extensions are
    /// dropped, taking the open `Box<dyn IngestSession>` with them. Nothing in
    /// `crates/silo/` implements `Drop`, so `IngestSession::abort` (which deletes the
    /// partial Parquet file) never runs and the half-written file is orphaned. No
    /// catalog damage — the snapshot is only committed at `CopyDone` — but the bytes
    /// leak, and a long-running server accumulates them. Fix by either implementing
    /// `Drop` (awkward: `abort` is async and takes `Box<Self>`) or sweeping
    /// unreferenced data files out-of-band, which fits with the background compaction
    /// already planned.
    pub async fn abort(&self, mut copy: CopyInFlight) {
        if let Some(session) = copy.session.take() {
            let _ = session.abort().await;
        }
    }

    async fn push_line(&self, copy: &mut CopyInFlight, line: &[u8]) -> Result<(), Error> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);

        // The end-of-data marker is part of the COPY wire format, not something psql
        // keeps to itself: when the rows come from a script file (`psql -f`) it sends
        // `\.` as COPY data and expects the backend to consume it. Piped stdin and the
        // Go ingest client never send one, which is why every earlier probe passed.
        // Postgres treats a lone `\.` as end-of-data in both text and CSV, so a field
        // that happens to spell it terminates there too — same as the real thing.
        if line == b"\\." {
            return Ok(());
        }

        let text = std::str::from_utf8(line).map_err(|e| {
            Error::wrap_invalid_input(e, CODE_INVALID_UTF8.clone(), "COPY data is not valid UTF-8")
        })?;

        if copy.header_pending {
            copy.header_pending = false;
            if copy.targets.is_none() {
                let names: Vec<String> = text.split(copy.delimiter).map(str::to_string).collect();
                copy.targets = Some(resolve_targets(&names, &copy.schema)?);
            }
            return Ok(());
        }

        let targets = copy.targets.as_deref().expect("targets are resolved before any data line");
        let fields: Vec<&str> = text.split(copy.delimiter).collect();
        if fields.len() != targets.len() {
            return Err(Error::new_invalid_input(
                CODE_COLUMN_COUNT_MISMATCH.clone(),
                format!(
                    "COPY row has {} fields but {} columns were declared",
                    fields.len(),
                    targets.len()
                ),
            ));
        }

        // Columns the COPY never mentions stay Null — that is what Postgres does, and
        // silo rejects it if the column is actually required.
        let mut row = vec![Value::Null; copy.schema.len()];
        for (&index, field) in targets.iter().zip(fields) {
            if field != copy.null_marker {
                row[index] = parse_value(field, &copy.schema[index])?;
            }
        }

        let session = match &mut copy.session {
            Some(session) => session,
            None => copy
                .session
                .insert(self.sink.lock().await.begin_write(&copy.stream).await?),
        };
        session.push(row).await?;
        copy.total += 1;
        Ok(())
    }
}

/// Statements warzone serves itself. Mirrors the two arms of [`Intercepter::visit`] —
/// a `COPY ... TO` or `COPY ... FROM 'file'` is not ours, and belongs to DuckDB.
fn is_intercepted(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::Copy {
                to: false,
                target: CopyTarget::Stdin,
                ..
            }
    )
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
/// Anything else is an explicit error: silently coercing an unsupported type into
/// `TEXT` would quietly corrupt the column for the life of the table.
fn to_column_type(sql_type: &SqlType, column: &str) -> Result<ColumnType, Error> {
    match sql_type {
        SqlType::Boolean | SqlType::Bool => Ok(ColumnType::Bool),
        // silo stores integers and floats 64-bit wide; the narrower SQL spellings are
        // accepted and widened rather than rejected.
        SqlType::SmallInt(_) | SqlType::Int(_) | SqlType::Integer(_) | SqlType::BigInt(_) => Ok(ColumnType::Int64),
        SqlType::Real | SqlType::Float(_) | SqlType::Double(_) | SqlType::DoublePrecision => Ok(ColumnType::Float64),
        SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) => Ok(ColumnType::String),
        other => Err(Error::new_unsupported(
            CODE_UNSUPPORTED_TYPE.clone(),
            format!("column '{column}': type {other} is not supported (use BOOLEAN, INT, BIGINT, REAL, DOUBLE PRECISION, or TEXT)"),
        )),
    }
}

/// Folds the two option lists `sqlparser` produces — modern `WITH (...)` and the
/// pre-9.0 legacy syntax psql still emits (`COPY t FROM STDIN CSV HEADER`) — into
/// `(delimiter, null_marker, header)`. The format picks the defaults; explicit
/// options override. Anything we cannot honour is an error, never a silent default:
/// an ignored `QUOTE` or `DELIMITER` parses the rows wrong and says nothing.
fn copy_format(
    options: &[CopyOption],
    legacy_options: &[CopyLegacyOption],
) -> Result<(char, String, bool), Error> {
    let mut csv = false;
    let mut delimiter: Option<char> = None;
    let mut null_marker: Option<String> = None;
    let mut header = false;

    for option in options {
        match option {
            CopyOption::Format(ident) => match ident.value.to_ascii_lowercase().as_str() {
                "csv" => csv = true,
                "text" => csv = false,
                other => {
                    return Err(unsupported(
                        &CODE_COPY_UNSUPPORTED,
                        format!("COPY FORMAT '{other}' (use text or csv)"),
                    ))
                }
            },
            CopyOption::Delimiter(ch) => delimiter = Some(*ch),
            CopyOption::Null(marker) => null_marker = Some(marker.clone()),
            CopyOption::Header(flag) => header = *flag,
            other => return Err(unsupported(&CODE_COPY_UNSUPPORTED, format!("COPY option {other}"))),
        }
    }

    for option in legacy_options {
        match option {
            CopyLegacyOption::Csv(csv_options) => {
                csv = true;
                for csv_option in csv_options {
                    match csv_option {
                        CopyLegacyCsvOption::Header => header = true,
                        other => {
                            return Err(unsupported(&CODE_COPY_UNSUPPORTED, format!("COPY option {other}")))
                        }
                    }
                }
            }
            CopyLegacyOption::Delimiter(ch) => delimiter = Some(*ch),
            CopyLegacyOption::Null(marker) => null_marker = Some(marker.clone()),
            CopyLegacyOption::Header => header = true,
            other => return Err(unsupported(&CODE_COPY_UNSUPPORTED, format!("COPY option {other}"))),
        }
    }

    let (default_delim, default_null) = if csv { (',', String::new()) } else { ('\t', "\\N".to_string()) };
    Ok((
        delimiter.unwrap_or(default_delim),
        null_marker.unwrap_or(default_null),
        header,
    ))
}

/// Map each named column to its position in the table. An unknown column name is an
/// error — Postgres would reject it, and silently dropping the field would shift
/// every subsequent one into the wrong column.
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

/// One wire field -> one `Value`, parsed as the column's *registered* type. No
/// inference: the type was fixed at `CREATE TABLE`. Inferring it from the data is
/// what silently turned a CSV `01234` into the integer `1234`.
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

fn unsupported(code: &Code, what: impl std::fmt::Display) -> Error {
    Error::new_unsupported(code.clone(), format!("{what} is not supported"))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use silo::config::{CatalogConfig, DestinationConfig, SinkConfig, StorageConfig};

    use super::*;

    /// A sink that records what was registered and hands back schemas, so dispatch
    /// can be asserted with no Iceberg catalog and no disk behind it. Implements
    /// silo's own [`TableSink`] — no new abstraction, just a second impl of a trait
    /// that already exists.
    #[derive(Default)]
    struct FakeSink {
        registered: Vec<(StreamId, Vec<Column>)>,
        /// Rows the COPY actually pushed, shared with the session so they survive the
        /// `Box<Self>` that `commit` consumes.
        pushed: Arc<std::sync::Mutex<Vec<Vec<Value>>>>,
    }

    struct FakeSession {
        pushed: Arc<std::sync::Mutex<Vec<Vec<Value>>>>,
    }

    #[async_trait]
    impl IngestSession for FakeSession {
        async fn push(&mut self, row: Vec<Value>) -> Result<(), Error> {
            self.pushed.lock().unwrap().push(row);
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), Error> {
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<(), Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl TableSink for FakeSink {
        async fn register(&mut self, stream: &StreamId, columns: &[Column]) -> Result<(), Error> {
            self.registered.push((stream.clone(), columns.to_vec()));
            Ok(())
        }

        async fn schema(&mut self, stream: &StreamId) -> Result<Option<Vec<Column>>, Error> {
            Ok(self
                .registered
                .iter()
                .find(|(registered, _)| registered == stream)
                .map(|(_, columns)| columns.clone()))
        }

        async fn begin_write(&mut self, _stream: &StreamId) -> Result<Box<dyn IngestSession>, Error> {
            Ok(Box::new(FakeSession {
                pushed: self.pushed.clone(),
            }))
        }

        async fn close(&mut self, _stream: &StreamId) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Take custody of the COPY the visitor handed back, exactly as a real transport
    /// does. Asserting on what the caller was *handed* is more honest than reaching into
    /// the Intercepter, which holds nothing.
    fn copy_of(outcome: Outcome) -> CopyInFlight {
        match outcome {
            Outcome::CopyIn(copy) => copy,
            _ => panic!("expected a COPY"),
        }
    }

    /// An all-memory querier: DuckDB in-process, no extensions fetched, no network.
    async fn querier() -> Arc<QueryEngine> {
        let config = SinkConfig {
            destinations: vec![DestinationConfig {
                name: "local".to_string(),
                catalog: CatalogConfig::Memory { warehouse: "file:///tmp/warehouse".to_string() },
                storage: StorageConfig::Memory,
            }],
            batch_size: 10_000,
        };
        Arc::new(QueryEngine::new(&config).await.expect("memory-only querier"))
    }

    async fn intercepter() -> Intercepter {
        recording().await.0
    }

    /// An Intercepter plus the row log its sink writes to, for the tests that push rows.
    async fn recording() -> (Intercepter, Arc<std::sync::Mutex<Vec<Vec<Value>>>>) {
        let sink = FakeSink::default();
        let pushed = sink.pushed.clone();
        let intercepter = Intercepter {
            sink: Arc::new(Mutex::new(sink)),
            querier: querier().await,
        };
        (intercepter, pushed)
    }

    /// Run `sql` as a CREATE TABLE, then read back what silo actually holds for it.
    async fn registered(sql: &str, stream: StreamId) -> Vec<Column> {
        let intercepter = intercepter().await;
        let outcome = intercepter.visit(sql).await.expect("valid create table");
        assert!(matches!(outcome, Outcome::Created));

        let mut sink = intercepter.sink.lock().await;
        sink.schema(&stream).await.unwrap().expect("registered")
    }

    #[tokio::test]
    async fn create_table_registers_the_lowered_columns_with_silo() {
        let columns = registered(
            "CREATE TABLE demo.events (id BIGINT NOT NULL, name TEXT, score DOUBLE PRECISION, ok BOOLEAN)",
            StreamId::new(["demo"], "events"),
        )
        .await;
        assert_eq!(
            columns,
            vec![
                Column::new("id", ColumnType::Int64, true),
                Column::new("name", ColumnType::String, false),
                Column::new("score", ColumnType::Float64, false),
                Column::new("ok", ColumnType::Bool, false),
            ]
        );
    }

    #[tokio::test]
    async fn if_not_exists_makes_a_duplicate_create_a_no_op() {
        let intercepter = intercepter().await;
        let sql = "CREATE TABLE IF NOT EXISTS demo.events (id BIGINT)";
        intercepter.visit(sql).await.expect("first create");
        intercepter.visit(sql).await.expect("second create is a no-op, not an error");
    }

    #[tokio::test]
    async fn unsupported_create_table_forms_are_rejected() {
        let intercepter = intercepter().await;
        assert!(intercepter.visit("CREATE TABLE demo.t (at TIMESTAMP)").await.is_err());
        assert!(intercepter.visit("CREATE OR REPLACE TABLE demo.t (id BIGINT)").await.is_err());
    }

    #[tokio::test]
    async fn copy_from_stdin_resolves_against_the_registered_schema() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT, score DOUBLE PRECISION)")
            .await
            .unwrap();

        // The transport is handed a COPY resolved against the registered schema. The
        // Intercepter keeps nothing.
        let copy = copy_of(
            intercepter
                .visit("COPY demo.events (score, id) FROM STDIN WITH (FORMAT csv)")
                .await
                .unwrap(),
        );
        assert_eq!(copy.width(), 2);
        assert_eq!(copy.stream, StreamId::new(["demo"], "events"));
        assert_eq!(copy.delimiter, ',');
        assert_eq!(copy.null_marker, "");
        // A subset column list still produces a full-width row; score is column 2.
        assert_eq!(copy.targets, Some(vec![2, 0]));
        assert_eq!(copy.schema.len(), 3);
    }

    /// No column list and no header: the wire fields are the table's columns.
    #[tokio::test]
    async fn a_bare_copy_targets_every_column_in_table_order() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let copy = copy_of(intercepter.visit("COPY demo.events FROM STDIN").await.unwrap());
        assert_eq!(copy.width(), 2);
        assert_eq!(copy.targets, Some(vec![0, 1]));
        // Text is the default format.
        assert_eq!(copy.delimiter, '\t');
        assert_eq!(copy.null_marker, "\\N");
    }

    /// psql's `\copy t from 'f' csv header` emits pre-9.0 syntax, which `sqlparser`
    /// reports in `legacy_options`, not `options`. Honouring only the modern list
    /// silently ingests these with the wrong delimiter.
    #[tokio::test]
    async fn legacy_copy_options_are_honoured() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let copy = copy_of(intercepter.visit("COPY demo.events (id, name) FROM STDIN CSV HEADER").await.unwrap());
        assert_eq!(copy.delimiter, ',');
        assert!(copy.header_pending);
        // The explicit column list already names the columns; the header is consumed and
        // discarded.
        assert_eq!(copy.width(), 2);

        let copy = copy_of(intercepter.visit("COPY demo.events (id) FROM STDIN DELIMITER '|'").await.unwrap());
        assert_eq!(copy.delimiter, '|');
    }

    /// With no column list, the CSV header is what names the columns — so they are not
    /// known until that line arrives, and the COPY-IN handshake advertises zero fields.
    #[tokio::test]
    async fn a_header_with_no_column_list_defers_the_columns() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let copy = copy_of(intercepter.visit("COPY demo.events FROM STDIN CSV HEADER").await.unwrap());
        assert!(copy.header_pending);
        assert_eq!(copy.targets, None);
        assert_eq!(copy.width(), 0);
    }

    /// Each COPY is resolved from its own statement and handed away. Nothing carries
    /// over: the `'|'` below cannot leak into the next COPY, because the Intercepter
    /// never held it. (The old `reset()` did not clear `delimiter` or `null_marker`.)
    #[tokio::test]
    async fn a_copy_carries_no_state_over_to_the_next_one() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let piped = copy_of(intercepter.visit("COPY demo.events (id, name) FROM STDIN DELIMITER '|'").await.unwrap());
        assert_eq!(piped.delimiter, '|');
        intercepter.abort(piped).await; // consumes it — there is no value left to leak

        // A plain CSV COPY on the same Intercepter gets CSV defaults, not a stale '|'.
        let csv = copy_of(intercepter.visit("COPY demo.events (id, name) FROM STDIN CSV").await.unwrap());
        assert_eq!(csv.delimiter, ',');
        assert_eq!(csv.null_marker, "");
    }

    /// psql puts the `\.` end-of-data marker on the wire as COPY data whenever the rows
    /// come from a script file (`psql -f`) — a real backend consumes it. Splitting it as
    /// a data row yields a single field, which is why *every* COPY in a `-f` script died
    /// with "COPY row has 1 fields but 2 columns were declared". Piped stdin and the Go
    /// ingest client never send one, so nothing else caught this.
    #[tokio::test]
    async fn the_end_of_data_marker_is_not_a_row() {
        let (intercepter, pushed) = recording().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let copy = copy_of(intercepter.visit("COPY demo.events (id, name) FROM STDIN DELIMITER '|'").await.unwrap());

        // Byte for byte what psql -f sends, marker and trailing newline included.
        let copy = intercepter
            .on_data(copy, b"1|one\n2|two\n\\.\n")
            .await
            .expect("the marker is consumed, not parsed as a row");
        assert_eq!(intercepter.finish(copy).await.unwrap(), 2);

        assert_eq!(
            *pushed.lock().unwrap(),
            vec![
                vec![Value::Int64(1), Value::String("one".to_string())],
                vec![Value::Int64(2), Value::String("two".to_string())],
            ]
        );
    }

    /// The marker also has to survive the other path into `push_line`: with no trailing
    /// newline it is never a complete line, so it sits in `leftover` until `finish`
    /// flushes it.
    #[tokio::test]
    async fn an_unterminated_end_of_data_marker_is_not_a_row_either() {
        let (intercepter, pushed) = recording().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT, name TEXT)").await.unwrap();

        let copy = copy_of(intercepter.visit("COPY demo.events (id, name) FROM STDIN CSV").await.unwrap());

        let copy = intercepter.on_data(copy, b"1,one\n\\.").await.unwrap();
        assert_eq!(intercepter.finish(copy).await.unwrap(), 1);
        assert_eq!(pushed.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn copy_options_we_cannot_honour_are_rejected() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT)").await.unwrap();

        for sql in [
            "COPY demo.events (id) FROM STDIN WITH (FORMAT csv, QUOTE '~')",
            "COPY demo.events (id) FROM STDIN CSV QUOTE '~'",
            "COPY demo.events (id) FROM STDIN BINARY",
            "COPY demo.events (id) FROM STDIN WITH (FORMAT parquet)",
        ] {
            assert!(intercepter.visit(sql).await.is_err(), "{sql} should be rejected");
        }
    }

    #[tokio::test]
    async fn copy_into_an_unregistered_table_is_an_error() {
        let intercepter = intercepter().await;
        let err = intercepter.visit("COPY demo.nope (id) FROM STDIN").await.err().expect("unregistered table");
        assert!(err.is_type(errors::Type::NotFound));
    }

    #[tokio::test]
    async fn a_column_the_table_does_not_have_is_rejected() {
        let intercepter = intercepter().await;
        intercepter.visit("CREATE TABLE demo.events (id BIGINT)").await.unwrap();
        assert!(intercepter.visit("COPY demo.events (id, nope) FROM STDIN").await.is_err());
    }

    #[tokio::test]
    async fn a_select_passes_through_to_the_querier() {
        let intercepter = intercepter().await;
        let outcome = intercepter.visit("SELECT 1").await.unwrap();
        assert!(matches!(outcome, Outcome::Rows(_)));
    }

    /// Only COPY *FROM STDIN* is ours; a COPY that writes elsewhere is DuckDB's.
    #[tokio::test]
    async fn a_copy_that_is_not_from_stdin_passes_through() {
        let intercepter = intercepter().await;
        // DuckDB rejects this one (no such table), but the point is that it reached
        // DuckDB at all rather than being intercepted.
        let err = intercepter.visit("COPY demo.events TO STDOUT").await.err().expect("duckdb rejects it");
        assert!(!err.is_type(errors::Type::Unsupported), "should not be intercepted: {err}");
    }

    #[tokio::test]
    async fn a_plain_batch_passes_through_whole() {
        let intercepter = intercepter().await;
        let outcome = intercepter.visit("SELECT 1; SELECT 2").await.unwrap();
        assert!(matches!(outcome, Outcome::Rows(_)));
    }

    /// Serving one of these would discard its siblings, or (for CREATE TABLE) register
    /// the table with DuckDB instead of silo.
    #[tokio::test]
    async fn an_intercepted_statement_inside_a_batch_is_rejected() {
        let intercepter = intercepter().await;
        assert!(intercepter.visit("SELECT 1; COPY demo.events (id) FROM STDIN").await.is_err());
        assert!(intercepter.visit("SELECT 1; CREATE TABLE demo.t (id BIGINT)").await.is_err());
    }

    /// sqlparser's PostgreSQL dialect is strictly narrower than DuckDB's. Erroring on
    /// a parse failure instead of passing through would break every `read_parquet`
    /// query in the project — including the ones `docs/development.md` tells users to
    /// run.
    #[tokio::test]
    async fn sql_sqlparser_cannot_parse_still_reaches_the_querier() {
        let sql = "SUMMARIZE SELECT 1 AS x";
        assert!(parse(sql).is_err(), "precondition: sqlparser cannot parse this");

        let intercepter = intercepter().await;
        let outcome = intercepter.visit(sql).await.expect("DuckDB runs it");
        assert!(matches!(outcome, Outcome::Rows(_)));
    }

    #[test]
    fn values_parse_as_the_registered_type_not_an_inferred_one() {
        let text = Column::new("zip", ColumnType::String, false);
        let long = Column::new("id", ColumnType::Int64, false);

        // The leading-zero bug: inference made this the integer 1234. A registered TEXT
        // column keeps the string intact.
        assert_eq!(parse_value("01234", &text).unwrap(), Value::String("01234".into()));
        assert_eq!(parse_value("42", &long).unwrap(), Value::Int64(42));
        assert!(parse_value("not-a-number", &long).is_err());
    }
}
