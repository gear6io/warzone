mod config;
mod flags;
use std::{fs::read_to_string, sync::LazyLock};

use config::MustConfig;
pub use config::{Config, HttpServerConfig, ServerConfig};
use errors::{Code, Error};
pub use flags::Args;

static CODE_FAILED_READ_CONFIG: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("config_read_failed"));
static CODE_FAILED_DESERIALIZE_CONFIG: LazyLock<Code> =
    LazyLock::new(|| Code::must_new("config_deserialize_failed"));

pub fn load_config(args: Args) -> Config {
    let yaml = read_to_string(args.config)
        .map_err(|err| {
            Error::wrap_invalid_input(
                err,
                CODE_FAILED_READ_CONFIG.clone(),
                "failed to read config file",
            )
        })
        .unwrap();
    let mut config: config::Config = serde_yaml::from_str(&yaml)
        .map_err(|err| {
            Error::wrap_invalid_input(
                err,
                CODE_FAILED_DESERIALIZE_CONFIG.clone(),
                "failed to unmarshal config file",
            )
        })
        .unwrap();

    config.apply_defaults();

    return config;
}
