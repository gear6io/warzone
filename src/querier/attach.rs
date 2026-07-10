use silo::config::{CatalogConfig, DestinationConfig, StorageConfig};

use super::error::CODE_UNSUPPORTED_CATALOG_PROP;
use errors::Error;

/// REST catalog `props` keys DuckDB's `ATTACH ... TYPE ICEBERG` can actually
/// express. Anything else in `CatalogConfig::Rest.props` is rejected loudly
/// rather than silently dropped — iceberg-rust's REST client accepts an
/// arbitrary props map, DuckDB's iceberg extension does not.
const KNOWN_REST_PROPS: &[&str] = &["token", "oauth2-server-uri", "warehouse", "credential", "scope"];

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

    for destination in destinations {
        let name = catalog_name(&destination.name);
        if let Some(secret) = s3_secret_statement(&name, destination.storage.clone())? {
            statements.push(secret);
        }
        // No `USE <catalog>`: DuckDB's bare `USE` needs a catalog with a default schema,
        // which iceberg REST catalogs (the only kind we ATTACH) don't have — it errors
        // "No catalog + schema named ...". Tables are addressed `<catalog>.<ns>.<table>`.
        statements.extend(attach_statements_for_catalog(&name, &destination.catalog)?);
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
        // DuckDB's S3 ENDPOINT is a bare `host:port`; it prepends the scheme itself
        // (https unless USE_SSL is false). A leading `http(s)://` here produces a broken
        // `https://http://host` URL, so strip it and drive USE_SSL from it. iceberg-rust
        // (silo's write path) wants the full URL, so config keeps the scheme; we strip here.
        let (host, use_ssl) = match endpoint.strip_prefix("http://") {
            Some(host) => (host, false),
            None => (endpoint.strip_prefix("https://").unwrap_or(&endpoint), true),
        };
        opts.push(format!("ENDPOINT '{}'", escape(host)));
        opts.push(format!("USE_SSL {use_ssl}"));
    }
    if let Some(key) = access_key_id {
        opts.push(format!("KEY_ID '{}'", escape(&key)));
    }
    if let Some(secret) = secret_access_key {
        opts.push(format!("SECRET '{}'", escape(&secret)));
    }

    Ok(Some(format!("CREATE SECRET {catalog}_s3 ({});", opts.join(", "))))
}

/// Statements to attach one REST/memory catalog. REST-with-OAuth returns two: a
/// `CREATE SECRET ... (TYPE ICEBERG, ...)` holding the client credentials, then the
/// `ATTACH` that references it by name — DuckDB's iceberg extension takes OAuth2
/// client-credentials only via a named secret, not inline.
fn attach_statements_for_catalog(catalog: &str, config: &CatalogConfig) -> Result<Vec<String>, Error> {
    match config {
        CatalogConfig::Memory { .. } => Ok(vec![]),
        CatalogConfig::Rest { uri, warehouse, props } => {
            for key in props.keys() {
                if !KNOWN_REST_PROPS.contains(&key.as_str()) {
                    return Err(Error::new_unsupported(
                        CODE_UNSUPPORTED_CATALOG_PROP.clone(),
                        format!("catalog prop '{key}' has no DuckDB iceberg ATTACH equivalent"),
                    ));
                }
            }
            let mut statements = Vec::new();
            let mut opts = vec!["TYPE ICEBERG".to_string(), format!("ENDPOINT '{}'", escape(uri))];
            if let Some(token) = props.get("token") {
                opts.push(format!("SECRET '{}'", escape(token)));
            } else if let Some(credential) = props.get("credential") {
                // iceberg REST `credential` is `client_id:client_secret`.
                let (client_id, client_secret) = credential.split_once(':').ok_or_else(|| {
                    Error::new_invalid_input(
                        CODE_UNSUPPORTED_CATALOG_PROP.clone(),
                        format!("catalog prop 'credential' must be 'client_id:client_secret', got '{credential}'"),
                    )
                })?;
                let mut secret_opts = vec![
                    "TYPE ICEBERG".to_string(),
                    format!("CLIENT_ID '{}'", escape(client_id)),
                    format!("CLIENT_SECRET '{}'", escape(client_secret)),
                ];
                if let Some(server_uri) = props.get("oauth2-server-uri") {
                    secret_opts.push(format!("OAUTH2_SERVER_URI '{}'", escape(server_uri)));
                }
                if let Some(scope) = props.get("scope") {
                    secret_opts.push(format!("OAUTH2_SCOPE '{}'", escape(scope)));
                }
                let secret_name = format!("{catalog}_iceberg");
                statements.push(format!("CREATE SECRET {secret_name} ({});", secret_opts.join(", ")));
                opts.push(format!("SECRET {secret_name}"));
            }
            statements.push(format!("ATTACH '{}' AS {catalog} ({});", escape(warehouse), opts.join(", ")));
            Ok(statements)
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
        // No `USE`: bare USE fails on iceberg REST catalogs (no default schema).
        assert!(statements.iter().all(|s| !s.starts_with("USE ")));
    }

    #[test]
    fn s3_endpoint_scheme_becomes_use_ssl() {
        let destinations = vec![dest(
            "d",
            CatalogConfig::Rest { uri: "http://c".into(), warehouse: "s3://w".into(), props: HashMap::new() },
            StorageConfig::S3 {
                bucket: "w".into(),
                region: None,
                endpoint: Some("http://localhost:8333".into()),
                path_style: true,
                access_key_id: None,
                secret_access_key: None,
            },
        )];
        let statements = attach_statements(&destinations).unwrap();
        let secret = statements.iter().find(|s| s.starts_with("CREATE SECRET d_s3")).unwrap();
        // scheme stripped from ENDPOINT, driven into USE_SSL — no `https://http://`.
        assert!(secret.contains("ENDPOINT 'localhost:8333'"), "{secret}");
        assert!(secret.contains("USE_SSL false"), "{secret}");
    }

    #[test]
    fn rest_catalog_with_credential_emits_oauth_secret() {
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "root:s3cr3t".to_string());
        props.insert("oauth2-server-uri".to_string(), "http://localhost:8181/oauth/tokens".to_string());
        props.insert("scope".to_string(), "PRINCIPAL_ROLE:ALL".to_string());
        let destinations = vec![dest(
            "primary",
            CatalogConfig::Rest { uri: "http://localhost:8181".into(), warehouse: "warzone".into(), props },
            StorageConfig::Memory,
        )];
        let statements = attach_statements(&destinations).unwrap();
        assert!(statements.iter().any(|s| s
            == "CREATE SECRET primary_iceberg (TYPE ICEBERG, CLIENT_ID 'root', CLIENT_SECRET 's3cr3t', OAUTH2_SERVER_URI 'http://localhost:8181/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL');"));
        assert!(statements
            .iter()
            .any(|s| s == "ATTACH 'warzone' AS primary (TYPE ICEBERG, ENDPOINT 'http://localhost:8181', SECRET primary_iceberg);"));
    }

    #[test]
    fn malformed_credential_is_rejected() {
        let mut props = HashMap::new();
        props.insert("credential".to_string(), "no-colon-here".to_string());
        let destinations = vec![dest(
            "primary",
            CatalogConfig::Rest { uri: "http://localhost:8181".into(), warehouse: "warzone".into(), props },
            StorageConfig::Memory,
        )];
        assert!(attach_statements(&destinations).is_err());
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
