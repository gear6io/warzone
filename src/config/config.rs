use serde::Deserialize;
use silo::config::SinkConfig;

const DEFAULT_HTTP_PORT: i32 = 3886;

pub trait MustConfig {
    fn apply_defaults(&mut self);
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub service: Vec<ServerConfig>,
    pub silo: SinkConfig,
}

impl MustConfig for Config {
    fn apply_defaults(&mut self) {
        // initialize default servers
        if self.service.len() == 0 {
            let mut default_servers: Vec<ServerConfig> =
                vec![ServerConfig::Http(HttpServerConfig::new())];

            for server_config in &mut default_servers {
                server_config.apply_defaults()
            }

            self.service = default_servers
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum ServerConfig {
    Http(HttpServerConfig),
    PgWire,
}

impl MustConfig for ServerConfig {
    fn apply_defaults(&mut self) {
        match self {
            ServerConfig::Http(http_config) => {
                http_config.apply_defaults();
            }
            ServerConfig::PgWire => {}
        }
    }
}

#[derive(Default, Debug, Deserialize)]
pub struct HttpServerConfig {
    pub port: i32,
}

impl HttpServerConfig {
    fn new() -> Self {
        Self::default()
    }
}

impl MustConfig for HttpServerConfig {
    fn apply_defaults(&mut self) {
        if self.port <= 0 {
            self.port = DEFAULT_HTTP_PORT
        }
    }
}
