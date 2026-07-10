mod config;
mod server;
mod silo;
use crate::{
    config::{load_config, ServerConfig},
    server::http::serve,
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

    for server in config.service {
        match server {
            ServerConfig::Http(http_config) => serve(http_config, state.clone()).await?,
        }
    }

    Ok(())
}
