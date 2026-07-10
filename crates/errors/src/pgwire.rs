//! Postgres SQLSTATE mapping, sibling to `http.rs`. Kept dependency-free
//! (raw `&str` codes) for the same reason `http.rs` returns a raw `u16`
//! instead of pulling in the `http` crate — this crate stays minimal.

use crate::Type;

// https://www.postgresql.org/docs/current/errcodes-appendix.html
const SQLSTATE_INVALID_PARAMETER_VALUE: &str = "22023";
const SQLSTATE_INTERNAL_ERROR: &str = "XX000";
const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
const SQLSTATE_DUPLICATE_OBJECT: &str = "42710";
const SQLSTATE_INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";
const SQLSTATE_QUERY_CANCELED: &str = "57014";
const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";

/// Maps an error's [`Type`] to a Postgres SQLSTATE code.
pub fn sqlstate(kind: Type) -> &'static str {
    match kind {
        Type::NotFound => SQLSTATE_UNDEFINED_TABLE,
        Type::InvalidInput => SQLSTATE_INVALID_PARAMETER_VALUE,
        Type::Unsupported | Type::MethodNotAllowed => SQLSTATE_FEATURE_NOT_SUPPORTED,
        Type::AlreadyExists => SQLSTATE_DUPLICATE_OBJECT,
        Type::Unauthenticated => SQLSTATE_INVALID_AUTHORIZATION_SPECIFICATION,
        Type::Forbidden => SQLSTATE_INSUFFICIENT_PRIVILEGE,
        Type::Canceled | Type::Timeout => SQLSTATE_QUERY_CANCELED,
        Type::TooManyRequests => SQLSTATE_TOO_MANY_CONNECTIONS,
        Type::Internal => SQLSTATE_INTERNAL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_mapping() {
        assert_eq!(sqlstate(Type::NotFound), "42P01");
        assert_eq!(sqlstate(Type::Unsupported), "0A000");
        assert_eq!(sqlstate(Type::Unauthenticated), "28000");
        assert_eq!(sqlstate(Type::Internal), "XX000");
    }
}
