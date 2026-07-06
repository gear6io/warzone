mod config;
mod server;
use crate::{
    config::{load_config, ServerConfig},
    server::http::serve,
};
use clap::Parser;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = config::Args::parse();
    let config = load_config(args);

    for server in config.service {
        match server {
            ServerConfig::Http(http_config) => serve(http_config).await?,
        }
    }

    Ok(())
}
