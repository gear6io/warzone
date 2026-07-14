use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::PgWireBackendMessage;

use errors::{Code, Error};
use silo::StreamId;
use silo::ingest::{Column, TableSink};

use super::error::to_pgwire_error;
use super::results::record_batches_to_query_response;
use super::{copy, ddl};
use crate::silo::AppState;

static CODE_COPY_UNKNOWN_TABLE: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("copy_unknown_table"));

pub struct Handler {
    state: AppState,
}

fn no_copy_in_progress() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "08P01".to_string(),
        "received COPY data with no COPY in progress".to_string(),
    )))
}

#[async_trait]
impl NoopStartupHandler for Handler {}

#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        // `CREATE TABLE` registers a schema with silo; `COPY ... FROM STDIN`
        // ingests against an already-registered one. Everything else (reads,
        // other DDL, ...) goes to the querier unchanged.
        if let Some(plan) = ddl::detect(query).map_err(|e| to_pgwire_error(&e))? {
            return self.create_table_old(plan).await;
        }
        if let Some(plan) = copy::detect(query).map_err(|e| to_pgwire_error(&e))? {
            return self.begin_copy(client, plan).await;
        }

        let result = self
            .state
            .querier
            .query(query)
            .await
            .map_err(|e| to_pgwire_error(&e))?;
        Ok(vec![Response::Query(record_batches_to_query_response(
            result,
        )?)])
    }
}

impl Handler {
    pub async fn create_table(&self, stream: &StreamId, columns: Vec<Column>, if_not_exists: bool) -> PgWireResult<Vec<Response>> {
        let mut sink = self.state.sink.lock().await;
        let already_registered = sink
            .schema(&stream)
            .await
            .map_err(|e| to_pgwire_error(&e))?
            .is_some();

        // `register` errors on a duplicate by itself; the lookup above only
        // exists so `IF NOT EXISTS` can turn that into a no-op.
        if !(if_not_exists && already_registered) {
            sink.register(&stream, &columns)
                .await
                .map_err(|e| to_pgwire_error(&e))?;
        }
        Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
    }

    pub async fn create_table_old(&self, plan: ddl::CreateTablePlan) -> PgWireResult<Vec<Response>> {
        let mut sink = self.state.sink.lock().await;
        let already_registered = sink
            .schema(&plan.stream)
            .await
            .map_err(|e| to_pgwire_error(&e))?
            .is_some();

        // `register` errors on a duplicate by itself; the lookup above only
        // exists so `IF NOT EXISTS` can turn that into a no-op.
        if !(plan.if_not_exists && already_registered) {
            sink.register(&plan.stream, &plan.columns)
                .await
                .map_err(|e| to_pgwire_error(&e))?;
        }
        Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
    }

    /// Resolve the COPY against the table's registered schema, then enter COPY-IN
    /// mode. An unregistered table is an error here — ingest never creates one.
    async fn begin_copy<C>(
        &self,
        client: &mut C,
        plan: copy::CopyPlan,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let schema = {
            let mut sink = self.state.sink.lock().await;
            sink.schema(&plan.stream)
                .await
                .map_err(|e| to_pgwire_error(&e))?
        };
        let schema = schema.ok_or_else(|| {
            to_pgwire_error(&Error::new_not_found(
                CODE_COPY_UNKNOWN_TABLE.clone(),
                format!(
                    "table {} does not exist — CREATE TABLE it first",
                    plan.stream
                ),
            ))
        })?;

        let state = copy::CopyState::new(plan, &schema).map_err(|e| to_pgwire_error(&e))?;
        let columns = state.columns;
        client.session_extensions().insert(state);
        Ok(vec![Response::CopyIn(CopyResponse::new(
            0,
            columns,
            stream::empty::<PgWireResult<CopyData>>(),
        ))])
    }
}

#[async_trait]
impl CopyHandler for Handler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = client
            .session_extensions()
            .get::<copy::CopyState>()
            .ok_or_else(no_copy_in_progress)?;
        let mut inner = state.inner.lock().await;
        if let Err(e) = inner
            .on_data(&self.state.sink, copy_data.data.as_ref())
            .await
        {
            inner.abort().await;
            return Err(to_pgwire_error(&e));
        }
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = client
            .session_extensions()
            .get::<copy::CopyState>()
            .ok_or_else(no_copy_in_progress)?;
        let mut inner = state.inner.lock().await;
        let total = match inner.finish(&self.state.sink).await {
            Ok(total) => total,
            Err(e) => {
                inner.abort().await;
                return Err(to_pgwire_error(&e));
            }
        };
        drop(inner);

        // The pgwire loop sends ReadyForQuery after this returns but not the
        // command tag, so emit `COPY <n>` ourselves.
        let tag = Tag::new("COPY").with_rows(total);
        client
            .send(PgWireBackendMessage::CommandComplete(tag.into()))
            .await?;
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(state) = client.session_extensions().get::<copy::CopyState>() {
            state.inner.lock().await.abort().await;
        }
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "57014".to_string(),
            format!("COPY aborted by client: {}", fail.message),
        )))
    }
}

pub struct Handlers {
    handler: Arc<Handler>,
}

impl Handlers {
    pub fn new(state: AppState) -> Self {
        Self {
            handler: Arc::new(Handler { state }),
        }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }
}
