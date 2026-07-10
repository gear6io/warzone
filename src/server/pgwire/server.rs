use std::sync::Arc;

use tokio::net::TcpListener;

use super::handlers::Handlers;
use crate::silo::AppState;

const ADDRESS: &str = "127.0.0.1";
const PORT: i32 = 5432;

pub async fn serve(state: AppState) -> std::io::Result<()> {
    let handlers = Arc::new(Handlers::new(state));

    let listener = TcpListener::bind(format!("{ADDRESS}:{PORT}")).await?;
    println!("PG-wire server running at {ADDRESS}:{PORT}");

    loop {
        let (socket, _) = listener.accept().await?;
        let handlers = handlers.clone();
        tokio::spawn(async move { pgwire::tokio::process_socket(socket, None, handlers).await });
    }
}
