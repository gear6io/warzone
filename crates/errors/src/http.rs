//! HTTP-status mapping and JSON response shape, ported from `http.go`. Uses
//! raw status codes (`u16`) instead of pulling in the `http` crate for
//! eleven constants.

use serde::Serialize;

use crate::{Error, Type};

// Only the subset of status codes Type maps to.
const STATUS_OK_INTERNAL_ERROR: u16 = 500;
const STATUS_NOT_FOUND: u16 = 404;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_CONFLICT: u16 = 409;
const STATUS_UNAUTHORIZED: u16 = 401;
const STATUS_FORBIDDEN: u16 = 403;
const STATUS_METHOD_NOT_ALLOWED: u16 = 405;
const STATUS_REQUEST_TIMEOUT: u16 = 408;
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
const STATUS_CLIENT_CLOSED_REQUEST: u16 = 499;

/// Maps an error's [`Type`] to an HTTP status code.
pub fn http_status(kind: Type) -> u16 {
    match kind {
        Type::NotFound => STATUS_NOT_FOUND,
        Type::InvalidInput | Type::Unsupported => STATUS_BAD_REQUEST,
        Type::AlreadyExists => STATUS_CONFLICT,
        Type::Unauthenticated => STATUS_UNAUTHORIZED,
        Type::Forbidden => STATUS_FORBIDDEN,
        Type::MethodNotAllowed => STATUS_METHOD_NOT_ALLOWED,
        Type::Timeout => STATUS_REQUEST_TIMEOUT,
        Type::TooManyRequests => STATUS_TOO_MANY_REQUESTS,
        Type::Canceled => STATUS_CLIENT_CLOSED_REQUEST,
        Type::Internal => STATUS_OK_INTERNAL_ERROR,
    }
}

/// The structured error response body. Arrays are never omitted/null
/// (OpenAPI-safe), matching Go's `nonNilStrings`.
#[derive(Debug, Serialize)]
pub struct Json {
    #[serde(rename = "type")]
    pub kind: String,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub errors: Vec<ErrorAdditional>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryJson>,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorAdditional {
    pub message: String,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RetryJson {
    pub delay_ms: u128,
}

/// Converts an [`Error`] to JSON. The `source` chain is emitted under
/// `errors` alongside `additional`.
pub fn as_json(err: &Error) -> Json {
    Json {
        kind: err.kind().to_string(),
        code: err.code().to_string(),
        message: err.message().to_string(),
        url: err.url().map(str::to_string),
        errors: err
            .additional()
            .iter()
            .map(|a| ErrorAdditional {
                message: a.message.clone(),
                suggestions: a.suggestions.clone(),
            })
            .chain(err.causes().into_iter().map(|message| ErrorAdditional {
                message,
                suggestions: Vec::new(),
            }))
            .collect(),
        retry: err
            .retry
            .map(|d| RetryJson { delay_ms: d.as_millis() }),
        suggestions: err.suggestions().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Code;
    use std::time::Duration;

    #[test]
    fn status_mapping() {
        assert_eq!(http_status(Type::NotFound), 404);
        assert_eq!(http_status(Type::InvalidInput), 400);
        assert_eq!(http_status(Type::AlreadyExists), 409);
        assert_eq!(http_status(Type::Unauthenticated), 401);
        assert_eq!(http_status(Type::Forbidden), 403);
        assert_eq!(http_status(Type::Internal), 500);
        assert_eq!(http_status(Type::Timeout), 408);
        assert_eq!(http_status(Type::TooManyRequests), 429);
        assert_eq!(http_status(Type::Canceled), 499);
    }

    #[test]
    fn as_json_null_safety() {
        let err = Error::new_internal(Code::INTERNAL, "oops");
        let j = as_json(&err);
        assert!(j.errors.is_empty());
        assert!(j.suggestions.is_empty());
    }

    #[test]
    fn retry_delay_round_trips() {
        let err = Error::new_too_many_requests(Code::TOO_MANY_REQUESTS, "slow down")
            .with_retry_after(Duration::from_secs(5));
        assert_eq!(err.retry_delay(), Duration::from_secs(5));
        let j = as_json(&err);
        assert_eq!(j.retry.unwrap().delay_ms, 5000);
    }

    #[test]
    fn wrapped_cause_reaches_json() {
        let duckdb_like = std::io::Error::other("Catalog Error: Table with name trips does not exist");
        let err = Error::wrap_invalid_input(duckdb_like, Code::INVALID_INPUT, "invalid query: select * from demo.trips");
        let j = as_json(&err);
        assert_eq!(j.message, "invalid query: select * from demo.trips");
        assert!(j.errors.iter().any(|e| e.message.contains("Catalog Error")));
    }

    #[test]
    fn nested_causes_are_not_duplicated() {
        let root = std::io::Error::other("root cause");
        let inner = Error::wrap_internal(root, Code::INTERNAL, "inner boundary");
        let outer = Error::wrap_invalid_input(inner, Code::INVALID_INPUT, "outer boundary");
        let messages: Vec<_> = as_json(&outer).errors.into_iter().map(|e| e.message).collect();
        assert_eq!(messages, vec!["inner boundary", "root cause"]);
    }

    #[test]
    fn hint_propagation_through_wrap() {
        let inner = Error::new_invalid_input(Code::INVALID_INPUT, "bad field")
            .with_suggestions(vec!["did you mean: `foo`".to_string()]);
        let outer = Error::wrap_invalid_input(inner, Code::INVALID_INPUT, "validation failed");
        let j = as_json(&outer);
        assert!(!j.suggestions.is_empty());
    }
}
