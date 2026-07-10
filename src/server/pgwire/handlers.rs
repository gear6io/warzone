use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::PgWireResult;

use super::error::to_pgwire_error;
use super::results::record_batches_to_query_response;
use crate::silo::AppState;

pub struct Handler {
    state: AppState,
}

#[async_trait]
impl NoopStartupHandler for Handler {}

#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let result = self.state.querier.query(query).await.map_err(|e| to_pgwire_error(&e))?;
        Ok(vec![Response::Query(record_batches_to_query_response(result)?)])
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
}
