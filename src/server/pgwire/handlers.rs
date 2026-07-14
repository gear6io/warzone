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
use tokio::sync::Mutex;

use errors::{Code, Error};

use super::error::to_pgwire_error;
use super::results::record_batches_to_query_response;
use crate::querier::intercepter::{CopyInFlight, Executor, Intercepter};
use crate::querier::QueryResult;
use crate::silo::AppState;

static CODE_RESULT_ENCODE_FAILED: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("query_result_encode_failed"));

/// The connection's COPY slot. `SessionExtensions` has no `remove`, so a finished COPY
/// is cleared by leaving `None` behind rather than by dropping the entry.
type CopySlot = Mutex<Option<CopyInFlight>>;

pub struct Handler {
    /// One for the whole server: the Intercepter is stateless, so there is nothing to
    /// keep per connection.
    intercepter: Intercepter,
}

fn no_copy_in_progress() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "08P01".to_string(),
        "received COPY data with no COPY in progress".to_string(),
    )))
}

/// Renders what the [`Intercepter`] did onto the Postgres wire, and takes custody of a
/// COPY when one starts. Only `&C` is needed: `session_extensions()` and its `insert`
/// both take `&self`.
struct PgExecutor<'a, C> {
    client: &'a C,
}

#[async_trait]
impl<C> Executor for PgExecutor<'_, C>
where
    C: ClientInfo + Sync,
{
    type Output = Vec<Response>;

    fn created(&mut self) -> Self::Output {
        vec![Response::Execution(Tag::new("CREATE TABLE"))]
    }

    fn rows(&mut self, result: QueryResult) -> Result<Self::Output, Error> {
        // This only fails when Arrow cannot render a value, which is ours, not the
        // caller's — so it becomes an Internal error rather than travelling back out as
        // an opaque protocol error.
        record_batches_to_query_response(result)
            .map(|response| vec![Response::Query(response)])
            .map_err(|e| {
                Error::wrap_internal(
                    e,
                    CODE_RESULT_ENCODE_FAILED.clone(),
                    "failed to encode query result",
                )
            })
    }

    /// Take custody of the COPY for the life of the connection. The Intercepter is
    /// stateless, so the connection is the only thing with the right lifetime.
    async fn copy_in(&mut self, copy: CopyInFlight) -> Result<Self::Output, Error> {
        let columns = copy.width();
        self.client
            .session_extensions()
            .insert::<CopySlot>(Mutex::new(Some(copy)));
        Ok(vec![Response::CopyIn(CopyResponse::new(
            0,
            columns,
            stream::empty::<PgWireResult<CopyData>>(),
        ))])
    }
}

impl Handler {
    fn copy_slot<C: ClientInfo>(&self, client: &C) -> PgWireResult<Arc<CopySlot>> {
        client
            .session_extensions()
            .get::<CopySlot>()
            .ok_or_else(no_copy_in_progress)
    }
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
        let mut executor = PgExecutor { client };
        self.intercepter
            .visit(&mut executor, query)
            .await
            .map_err(|e| to_pgwire_error(&e))
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
        let slot = self.copy_slot(client)?;
        let mut slot = slot.lock().await;
        let copy = slot.take().ok_or_else(no_copy_in_progress)?;

        // Handed back only if it survived. A failed COPY was aborted inside `on_data`
        // and is gone — the slot stays empty, so further data is rejected.
        match self.intercepter.on_data(copy, copy_data.data.as_ref()).await {
            Ok(copy) => {
                *slot = Some(copy);
                Ok(())
            }
            Err(e) => Err(to_pgwire_error(&e)),
        }
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let slot = self.copy_slot(client)?;
        let copy = slot.lock().await.take().ok_or_else(no_copy_in_progress)?;
        let total = self
            .intercepter
            .finish(copy)
            .await
            .map_err(|e| to_pgwire_error(&e))?;

        // The pgwire loop sends ReadyForQuery after this returns but not the command
        // tag, so emit `COPY <n>` ourselves.
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
        if let Some(slot) = client.session_extensions().get::<CopySlot>() {
            let copy = slot.lock().await.take();
            if let Some(copy) = copy {
                self.intercepter.abort(copy).await;
            }
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
            handler: Arc::new(Handler {
                intercepter: Intercepter::new(&state),
            }),
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
