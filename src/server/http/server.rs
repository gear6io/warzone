use axum::{response::Html, routing::get, Router};
use tokio::net::TcpListener;

use crate::{config::HttpServerConfig, server::http::v1, silo::AppState};

const ADDRESS: &str = "127.0.0.1";

/// The SQL console, compiled into the binary and served same-origin — the page talks
/// to the API it is served from, so there is no CORS layer to open up on an endpoint
/// that runs arbitrary DuckDB SQL unauthenticated.
const PLAY_HTML: &str = include_str!("../../../ui/play.html");

pub async fn serve(config: &HttpServerConfig, state: AppState) -> std::io::Result<()> {
    let router = api_router(state);

    let listener = TcpListener::bind(format!("{ADDRESS}:{}", config.port)).await?;

    println!("Server running at http://{ADDRESS}:{}/play", config.port);
    axum::serve(listener, router).await
}

async fn play() -> Html<&'static str> {
    Html(PLAY_HTML)
}

fn api_router(state: AppState) -> Router {
    let v1 = v1::router(state);
    Router::new().nest("/api", v1).route("/play", get(play))
}
