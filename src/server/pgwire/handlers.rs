use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::PgWireResult;

use super::error::query_engine_not_implemented;
use crate::silo::AppState;

// ponytail: state isn't read yet — no query engine exists to hand it to.
// Kept on the handler so the query-engine task can start reading tables
// through the same `AppState` the write path already uses.
#[allow(dead_code)]
pub struct Handler {
    state: AppState,
}

#[async_trait]
impl NoopStartupHandler for Handler {}

#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, _client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        Err(query_engine_not_implemented())
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
