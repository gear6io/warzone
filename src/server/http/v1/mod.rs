use axum::{Router, routing::{get, post}};
mod payload;
mod healthcheck;

pub fn router() -> Router {
    let mut router = Router::new();

    let routes = vec![
        ("/insert", post(payload::accept_payload)),
        ("/health", get(healthcheck::ready)),
    ];

    for (path, method_router) in routes {
        router = router.route(path, method_router);
    }
    
    return Router::new().nest("/v1", router)
}
