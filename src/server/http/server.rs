use axum::Router;
use tokio::net::TcpListener;

use crate::{config::HttpServerConfig, server::http::v1};

const ADDRESS: &str = "127.0.0.1";

pub async fn serve(config: HttpServerConfig) -> std::io::Result<()> {
    let router = api_router();

    let listener = TcpListener::bind(format!("{ADDRESS}:{}", config.port)).await?;

    println!("Server running at {ADDRESS}");
    axum::serve(listener, router).await
}

fn api_router() -> Router {
    let v1 = v1::router();
    return Router::new().nest("/api", v1)
}