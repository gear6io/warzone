use axum::Router;
use tokio::net::TcpListener;

use crate::server::http::v1;

const ADDRESS: &str = "127.0.0.1:3000";

pub async fn serve() -> std::io::Result<()> {
    let router = api_router();

    let listener = TcpListener::bind(ADDRESS).await?;

    println!("Server running at {ADDRESS}");
    axum::serve(listener, router).await
}

fn api_router() -> Router {
    let v1 = v1::router();
    return Router::new().nest("/api", v1)
}