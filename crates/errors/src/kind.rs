use std::borrow::Cow;
use std::fmt;

use crate::Error;

/// The category of an error. Drives [`crate::Error::http_status`] and lets
/// callers branch on failure kind without matching on `Code` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    InvalidInput,
    Internal,
    Unsupported,
    NotFound,
    MethodNotAllowed,
    AlreadyExists,
    Unauthenticated,
    Forbidden,
    Canceled,
    Timeout,
    TooManyRequests,
}

impl Type {
    pub fn as_str(&self) -> &'static str {
        match self {
            Type::InvalidInput => "invalid-input",
            Type::Internal => "internal",
            Type::Unsupported => "unsupported",
            Type::NotFound => "not-found",
            Type::MethodNotAllowed => "method-not-allowed",
            Type::AlreadyExists => "already-exists",
            Type::Unauthenticated => "unauthenticated",
            Type::Forbidden => "forbidden",
            Type::Canceled => "canceled",
            Type::Timeout => "timeout",
            Type::TooManyRequests => "too-many-requests",
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A machine-readable error code, distinct from [`Type`]: many codes can
/// share one type. Validated to `[a-z_]+` on construction, same as the Go
/// reference (`code.go`'s `codeRegex`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Code(Cow<'static, str>);

impl Code {
    pub const INVALID_INPUT: Code = Code(Cow::Borrowed("invalid_input"));
    pub const INTERNAL: Code = Code(Cow::Borrowed("internal"));
    pub const UNSUPPORTED: Code = Code(Cow::Borrowed("unsupported"));
    pub const NOT_FOUND: Code = Code(Cow::Borrowed("not_found"));
    pub const METHOD_NOT_ALLOWED: Code = Code(Cow::Borrowed("method_not_allowed"));
    pub const ALREADY_EXISTS: Code = Code(Cow::Borrowed("already_exists"));
    pub const UNAUTHENTICATED: Code = Code(Cow::Borrowed("unauthenticated"));
    pub const FORBIDDEN: Code = Code(Cow::Borrowed("forbidden"));
    pub const CANCELED: Code = Code(Cow::Borrowed("canceled"));
    pub const TIMEOUT: Code = Code(Cow::Borrowed("timeout"));
    pub const TOO_MANY_REQUESTS: Code = Code(Cow::Borrowed("too_many_requests"));
    pub const UNKNOWN: Code = Code(Cow::Borrowed("unknown"));
    /// Reverse-engineered from a response lacking a code. Its presence in
    /// production code is a bug (same caveat as the Go `CodeUnset`).
    pub const UNSET: Code = Code(Cow::Borrowed("unset"));

    /// Builds a custom code, rejecting anything outside `[a-z_]+`.
    pub fn new(s: impl Into<String>) -> Result<Code, Error> {
        let s = s.into();
        if s.is_empty() || !s.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            return Err(Error::new_invalid_input(
                Code::INVALID_INPUT,
                format!("invalid code: {s}"),
            ));
        }
        Ok(Code(Cow::Owned(s)))
    }

    /// Like [`Code::new`] but panics on invalid input.
    pub fn must_new(s: impl Into<String>) -> Code {
        Code::new(s).expect("invalid code")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_validation() {
        assert!(Code::new("valid_code").is_ok());
        assert!(Code::new("Invalid").is_err());
        assert!(Code::new("has space").is_err());
        assert!(Code::new("").is_err());
    }
}
