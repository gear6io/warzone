use silo::config::{CatalogConfig, DestinationConfig, StorageConfig};

use super::error::CODE_UNSUPPORTED_CATALOG_PROP;
use errors::Error;

/// REST catalog `props` keys DuckDB's `ATTACH ... TYPE ICEBERG` can actually
/// express. Anything else in `CatalogConfig::Rest.props` is rejected loudly
/// rather than silently dropped — iceberg-rust's REST client accepts an
/// arbitrary props map, DuckDB's iceberg extension does not.
const KNOWN_REST_PROPS: &[&str] = &["token", "oauth2-server-uri", "warehouse"];

/// Turns a valid DuckDB identifier out of a destination name (spaces/dashes
/// etc. aren't valid unquoted identifiers, and we don't want to have to
/// think about quoting rules everywhere `<name>.ns.table` gets built).
pub(crate) fn catalog_name(destination: &str) -> String {
    destination
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c.to_ascii_lowercase() } else { '_' })
        .collect()
}

/// Builds the ordered list of SQL statements needed to attach every
/// destination as its own DuckDB catalog. Statement order matters:
/// extensions must load before any `CREATE SECRET`/`ATTACH`.
pub(crate) fn attach_statements(destinations: &[DestinationConfig]) -> Result<Vec<String>, Error> {
    // Only pull in `iceberg`/`httpfs` when a destination actually needs them
    // (`CatalogConfig::Memory`+`StorageConfig::Memory` needs neither) — both
    // extensions load over the network on first use, so an all-memory config
    // (the common case for unit tests) stays network-free.
    let needs_iceberg = destinations.iter().any(|d| matches!(d.catalog, CatalogConfig::Rest { .. }));
    let needs_httpfs = destinations.iter().any(|d| matches!(d.storage, StorageConfig::S3 { .. } | StorageConfig::Minio { .. }));

    let mut statements = Vec::new();
    if needs_iceberg {
        statements.push("INSTALL iceberg;".to_string());
        statements.push("LOAD iceberg;".to_string());
    }
    if needs_httpfs {
        statements.push("INSTALL httpfs;".to_string());
        statements.push("LOAD httpfs;".to_string());
    }

    for (i, destination) in destinations.iter().enumerate() {
        let name = catalog_name(&destination.name);
        if let Some(secret) = s3_secret_statement(&name, destination.storage.clone())? {
            statements.push(secret);
        }
        if let Some(attach) = attach_statement(&name, &destination.catalog)? {
            statements.push(attach);
            if i == 0 {
                statements.push(format!("USE {name};"));
            }
        }
    }

    Ok(statements)
}

fn s3_secret_statement(catalog: &str, storage: StorageConfig) -> Result<Option<String>, Error> {
    let StorageConfig::S3 { bucket: _, region, endpoint, path_style, access_key_id, secret_access_key } =
        storage.into_resolved_s3()
    else {
        return Ok(None);
    };

    let mut opts = vec![format!("TYPE s3"), format!("URL_STYLE '{}'", if path_style { "path" } else { "vhost" })];
    if let Some(region) = region {
        opts.push(format!("REGION '{}'", escape(&region)));
    }
    if let Some(endpoint) = endpoint {
        opts.push(format!("ENDPOINT '{}'", escape(&endpoint)));
    }
    if let Some(key) = access_key_id {
        opts.push(format!("KEY_ID '{}'", escape(&key)));
    }
    if let Some(secret) = secret_access_key {
        opts.push(format!("SECRET '{}'", escape(&secret)));
    }

    Ok(Some(format!("CREATE SECRET {catalog}_s3 ({});", opts.join(", "))))
}

fn attach_statement(catalog: &str, config: &CatalogConfig) -> Result<Option<String>, Error> {
    match config {
        CatalogConfig::Memory { .. } => Ok(None),
        CatalogConfig::Rest { uri, warehouse, props } => {
            for key in props.keys() {
                if !KNOWN_REST_PROPS.contains(&key.as_str()) {
                    return Err(Error::new_unsupported(
                        CODE_UNSUPPORTED_CATALOG_PROP.clone(),
                        format!("catalog prop '{key}' has no DuckDB iceberg ATTACH equivalent"),
                    ));
                }
            }
            let mut opts = vec!["TYPE ICEBERG".to_string(), format!("ENDPOINT '{}'", escape(uri))];
            if let Some(token) = props.get("token") {
                opts.push(format!("SECRET '{}'", escape(token)));
            }
            Ok(Some(format!("ATTACH '{}' AS {catalog} ({});", escape(warehouse), opts.join(", "))))
        }
    }
}

fn escape(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use silo::config::{CatalogConfig, DestinationConfig, StorageConfig};

    use super::*;

    fn dest(name: &str, catalog: CatalogConfig, storage: StorageConfig) -> DestinationConfig {
        DestinationConfig { name: name.to_string(), catalog, storage }
    }

    #[test]
    fn memory_catalog_has_no_attach_statement() {
        let destinations = vec![dest("local", CatalogConfig::Memory { warehouse: "file:///tmp/wh".into() }, StorageConfig::Memory)];
        let statements = attach_statements(&destinations).unwrap();
        assert!(statements.iter().all(|s| !s.starts_with("ATTACH")));
    }

    #[test]
    fn rest_catalog_with_s3_emits_secret_and_attach() {
        let destinations = vec![dest(
            "primary-s3",
            CatalogConfig::Rest { uri: "http://localhost:8181".into(), warehouse: "s3://warehouse".into(), props: HashMap::new() },
            StorageConfig::S3 {
                bucket: "warehouse".into(),
                region: Some("us-east-1".into()),
                endpoint: None,
                path_style: false,
                access_key_id: Some("AKIA".into()),
                secret_access_key: Some("secret".into()),
            },
        )];
        let statements = attach_statements(&destinations).unwrap();
        assert!(statements.iter().any(|s| s.starts_with("CREATE SECRET primary_s3_s3")));
        assert!(statements.iter().any(|s| s == "ATTACH 's3://warehouse' AS primary_s3 (TYPE ICEBERG, ENDPOINT 'http://localhost:8181');"));
        assert!(statements.contains(&"USE primary_s3;".to_string()));
    }

    #[test]
    fn unknown_rest_prop_is_rejected() {
        let mut props = HashMap::new();
        props.insert("some-custom-knob".to_string(), "x".to_string());
        let destinations = vec![dest(
            "primary",
            CatalogConfig::Rest { uri: "http://localhost:8181".into(), warehouse: "s3://warehouse".into(), props },
            StorageConfig::Memory,
        )];
        let err = attach_statements(&destinations).unwrap_err();
        assert!(err.is_code(&CODE_UNSUPPORTED_CATALOG_PROP));
    }

    #[test]
    fn catalog_name_sanitizes_special_chars() {
        assert_eq!(catalog_name("primary-s3"), "primary_s3");
        assert_eq!(catalog_name("Primary S3"), "primary_s3");
    }
}
