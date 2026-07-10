use axum::{Router, routing::{get, post}};
use crate::silo::AppState;
mod payload;
mod healthcheck;
mod query;

pub fn router(state: AppState) -> Router {
    let mut router: Router<AppState> = Router::new();

    let routes = vec![
        ("/insert", post(payload::accept_payload)),
        ("/query", post(query::run_query)),
        ("/health", get(healthcheck::ready)),
    ];

    for (path, method_router) in routes {
        router = router.route(path, method_router);
    }

    let router = router.with_state(state);

    return Router::new().nest("/v1", router)
}
