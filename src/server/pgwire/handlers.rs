use std::fmt::Debug;
use std::sync::Arc;

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

use super::copy;
use super::error::to_pgwire_error;
use super::results::record_batches_to_query_response;
use crate::silo::AppState;

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
        // Intercept `COPY <table> FROM STDIN` and route it into the silo sink;
        // everything else (reads, DDL, ...) goes to the querier unchanged.
        match copy::detect(query).map_err(|e| to_pgwire_error(&e))? {
            Some(state) => {
                let columns = state.columns;
                client.session_extensions().insert(state);
                let response = CopyResponse::new(0, columns, stream::empty::<PgWireResult<CopyData>>());
                Ok(vec![Response::CopyIn(response)])
            }
            None => {
                let result = self.state.querier.query(query).await.map_err(|e| to_pgwire_error(&e))?;
                Ok(vec![Response::Query(record_batches_to_query_response(result)?)])
            }
        }
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
        let state = client.session_extensions().get::<copy::CopyState>().ok_or_else(no_copy_in_progress)?;
        let mut inner = state.inner.lock().await;
        if let Err(e) = inner.on_data(&self.state.sink, copy_data.data.as_ref()).await {
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
        let state = client.session_extensions().get::<copy::CopyState>().ok_or_else(no_copy_in_progress)?;
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
        client.send(PgWireBackendMessage::CommandComplete(tag.into())).await?;
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
        Self { handler: Arc::new(Handler { state }) }
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
