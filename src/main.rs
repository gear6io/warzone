mod config;
mod querier;
mod server;
mod silo;
use crate::{
    config::{load_config, ServerConfig},
    server::{http, pgwire as pgwire_server},
    silo::AppState,
};
use clap::Parser;
use std::sync::Arc;
use tokio::{sync::Mutex, task::JoinHandle};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = config::Args::parse();
    let config = load_config(args);

    let sink = silo::build_sink(&config.silo)
        .await
        .expect("failed to build silo sink");
    let querier = crate::querier::QueryEngine::new(&config.silo)
        .await
        .expect("failed to build query engine");
    let state = AppState {
        sink: Arc::new(Mutex::new(sink)),
        querier: Arc::new(querier),
    };

    let handlers: Vec<JoinHandle<Result<(), std::io::Error>>> = config
        .service
        .into_iter()
        .map(|config| {
            // TODO: no state cloning; Remove it later.
            let state = state.clone();
            tokio::spawn(async move {
                match config {
                    ServerConfig::Http(cfg) => http::serve(&cfg, state).await,
                    ServerConfig::PgWire => pgwire_server::serve(state).await,
                }
            })
        })
        .collect();

    for handle in handlers {
        handle.await.expect("server task panicked")?;
    }

    Ok(())
}
