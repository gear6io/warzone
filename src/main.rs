mod config;
mod server;
mod silo;
use crate::{
    config::{load_config, ServerConfig},
    server::{http, pgwire as pgwire_server},
    silo::AppState,
};
use clap::Parser;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = config::Args::parse();
    let config = load_config(args);

    let sink = silo::build_sink(&config.silo).await.expect("failed to build silo sink");
    let state = AppState { sink: Arc::new(Mutex::new(sink)) };

    let mut handles = Vec::new();
    for server in config.service {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            match server {
                ServerConfig::Http(cfg) => http::serve(cfg, state).await,
                ServerConfig::PgWire => pgwire_server::serve(state).await,
            }
        }));
    }
    for handle in handles {
        handle.await.expect("server task panicked")?;
    }

    Ok(())
}
