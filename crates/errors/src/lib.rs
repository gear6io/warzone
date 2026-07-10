//! `errors` is the single error-handling layer for this project. It is
//! adapted from `pragmata/pkg/errors` (Go) and should be used everywhere
//! instead of ad-hoc `String` errors or `Box<dyn std::error::Error>`.
//!
//! # Choosing `new` vs `wrap`
//!
//! Use [`Error::new`] / the `new_*` shortcuts (e.g. [`Error::new_not_found`])
//! when there is no underlying error to preserve — you are originating an
//! error:
//!
//! ```
//! use errors::{Error, Code};
//! # fn check(name: &str) -> Result<(), Error> {
//! if name.is_empty() {
//!     return Err(Error::new_invalid_input(Code::INVALID_INPUT, "name is required"));
//! }
//! # Ok(()) }
//! ```
//!
//! Use [`Error::wrap`] / the `wrap_*` shortcuts (e.g. [`Error::wrap_internal`])
//! ONLY at system boundaries — when you received an untyped error from an
//! external crate or stdlib and need to assign a type, code, and message.
//!
//! # Never re-wrap an already-typed error
//!
//! If the error came from another internal function that already returns
//! [`Error`], don't wrap it again — that overwrites its type and code. Use
//! [`Error::with_additional`] to attach context without altering type, code,
//! or message.
//!
//! # Inspecting errors
//!
//! Use [`Error::is_type`] and [`Error::is_code`] to branch on error type or
//! code. [`Error::source`] (via [`std::error::Error`]) walks the wrap chain,
//! same as any other std error.

mod http;
mod kind;
mod pgwire;
mod suggestions;

use std::fmt;
use std::time::Duration;

pub use http::{as_json, http_status, ErrorAdditional, Json, RetryJson};
pub use kind::{Code, Type};
pub use pgwire::sqlstate;
pub use suggestions::{
    closest_levenshtein_match, new_suggestions_from_func, new_suggestions_on_levenshtein_distance,
    new_valid_references, NOUN_FIELDS, NOUN_KEYS, NOUN_REFERENCES,
};

/// A single supplementary error detail with optional suggestions.
#[derive(Debug, Clone)]
pub struct Additional {
    pub(crate) message: String,
    pub(crate) suggestions: Vec<String>,
}

/// Captured stacktrace, either taken automatically at construction or
/// supplied verbatim via [`Error::with_stacktrace`].
enum Trace {
    Captured(std::backtrace::Backtrace),
    Raw(String),
}

impl Trace {
    fn render(&self) -> String {
        match self {
            Trace::Captured(bt) => bt.to_string(),
            Trace::Raw(s) => s.clone(),
        }
    }
}

/// The crate's error type: a type + code + message, optionally wrapping a
/// cause, plus retry/suggestion/URL metadata and a stacktrace.
pub struct Error {
    kind: Type,
    code: Code,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    url: Option<String>,
    additional: Vec<Additional>,
    trace: Trace,
    pub(crate) retry: Option<Duration>,
    suggestions: Vec<String>,
}

impl Error {
    pub fn new(kind: Type, code: Code, message: impl Into<String>) -> Error {
        Error {
            kind,
            code,
            message: message.into(),
            source: None,
            url: None,
            additional: Vec::new(),
            trace: Trace::Captured(std::backtrace::Backtrace::force_capture()),
            retry: None,
            suggestions: Vec::new(),
        }
    }

    /// Wraps `cause` as the origin of a new, typed `Error`. If `cause` is
    /// itself an `Error`, its retry/additional/suggestion hints survive the
    /// re-wrap (mirrors Go's `propagateHints`) so they aren't lost when a
    /// caller assigns a fresh type/code at a boundary.
    pub fn wrap(
        cause: impl std::error::Error + Send + Sync + 'static,
        kind: Type,
        code: Code,
        message: impl Into<String>,
    ) -> Error {
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(cause);
        let (retry, additional, suggestions) = match boxed.downcast_ref::<Error>() {
            Some(inner) => (inner.retry, inner.additional.clone(), inner.suggestions.clone()),
            None => (None, Vec::new(), Vec::new()),
        };
        Error {
            kind,
            code,
            message: message.into(),
            source: Some(boxed),
            url: None,
            additional,
            trace: Trace::Captured(std::backtrace::Backtrace::force_capture()),
            retry,
            suggestions,
        }
    }

    /// Attaches context to `cause` without altering its type, code, or
    /// message — the right call when `cause` already came from this crate
    /// (see the module docs' "never re-wrap" section). Falls back to
    /// `Type::Internal`/`Code::UNKNOWN` when `cause` isn't an `Error`.
    pub fn with_additional(
        cause: impl std::error::Error + Send + Sync + 'static,
        message: impl Into<String>,
    ) -> Error {
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(cause);
        let mut err = match boxed.downcast::<Error>() {
            Ok(inner) => *inner,
            Err(boxed) => {
                let text = boxed.to_string();
                Error {
                    kind: Type::Internal,
                    code: Code::UNKNOWN,
                    message: text,
                    source: Some(boxed),
                    url: None,
                    additional: Vec::new(),
                    trace: Trace::Captured(std::backtrace::Backtrace::force_capture()),
                    retry: None,
                    suggestions: Vec::new(),
                }
            }
        };
        err.additional.push(Additional {
            message: message.into(),
            suggestions: Vec::new(),
        });
        err
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn with_additional_message(mut self, message: impl Into<String>) -> Self {
        self.additional.push(Additional {
            message: message.into(),
            suggestions: Vec::new(),
        });
        self
    }

    pub fn with_suggestive_additional(
        mut self,
        message: impl Into<String>,
        suggestions: Vec<String>,
    ) -> Self {
        self.additional.push(Additional {
            message: message.into(),
            suggestions,
        });
        self
    }

    pub fn with_suggestions(mut self, suggestions: Vec<String>) -> Self {
        self.suggestions = suggestions;
        self
    }

    pub fn with_retry_after(mut self, delay: Duration) -> Self {
        self.retry = Some(delay);
        self
    }

    /// Replaces the auto-captured stacktrace with a pre-formatted string.
    pub fn with_stacktrace(mut self, s: impl Into<String>) -> Self {
        self.trace = Trace::Raw(s.into());
        self
    }

    pub fn stacktrace(&self) -> String {
        self.trace.render()
    }

    pub fn kind(&self) -> Type {
        self.kind
    }

    pub fn code(&self) -> &Code {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    pub fn additional(&self) -> &[Additional] {
        &self.additional
    }

    pub fn suggestions(&self) -> &[String] {
        &self.suggestions
    }

    /// Explicit retry delay, or `Duration::ZERO` when none was set.
    pub fn retry_delay(&self) -> Duration {
        self.retry.unwrap_or(Duration::ZERO)
    }

    /// Checks if the error matches the specified type.
    pub fn is_type(&self, t: Type) -> bool {
        self.kind == t
    }

    /// Checks if the error matches the specified code.
    pub fn is_code(&self, code: &Code) -> bool {
        &self.code == code
    }

    pub fn http_status(&self) -> u16 {
        http_status(self.kind)
    }

    pub fn as_json(&self) -> Json {
        as_json(self)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(src) => write!(f, "{src}"),
            None => f.write_str(&self.message),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("code", &self.code)
            .field("message", &self.message)
            .field("source", &self.source)
            .finish()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

macro_rules! shortcuts {
    ($( $type_variant:ident => $new_fn:ident, $wrap_fn:ident );* $(;)?) => {
        impl Error {
            $(
                pub fn $new_fn(code: Code, message: impl Into<String>) -> Error {
                    Error::new(Type::$type_variant, code, message)
                }
                pub fn $wrap_fn(
                    cause: impl std::error::Error + Send + Sync + 'static,
                    code: Code,
                    message: impl Into<String>,
                ) -> Error {
                    Error::wrap(cause, Type::$type_variant, code, message)
                }
            )*
        }
    };
}

shortcuts! {
    InvalidInput => new_invalid_input, wrap_invalid_input;
    Internal => new_internal, wrap_internal;
    NotFound => new_not_found, wrap_not_found;
    AlreadyExists => new_already_exists, wrap_already_exists;
    Unauthenticated => new_unauthenticated, wrap_unauthenticated;
    Forbidden => new_forbidden, wrap_forbidden;
    Unsupported => new_unsupported, wrap_unsupported;
    Timeout => new_timeout, wrap_timeout;
    Canceled => new_canceled, wrap_canceled;
    TooManyRequests => new_too_many_requests, wrap_too_many_requests;
}

// Go's MethodNotAllowed only ever had a New, no Wrap — kept asymmetric here too.
impl Error {
    pub fn new_method_not_allowed(code: Code, message: impl Into<String>) -> Error {
        Error::new(Type::MethodNotAllowed, code, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_chain_reaches_inner() {
        let inner = Error::new_internal(Code::INTERNAL, "db error");
        let inner_msg = inner.message().to_string();
        let outer = Error::wrap_internal(inner, Code::INTERNAL, "handler error");
        // Display surfaces the innermost message, same as Go's Error().
        assert_eq!(outer.to_string(), inner_msg);
        assert!(std::error::Error::source(&outer).is_some());
    }

    #[test]
    fn wrapping_plain_std_error_still_reachable() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing file");
        let wrapped = Error::wrap_not_found(io_err, Code::NOT_FOUND, "config not found");
        let src = std::error::Error::source(&wrapped).expect("source present");
        assert!(src.to_string().contains("missing file"));
    }

    #[test]
    fn stacktrace_capture_and_override() {
        let err = Error::new_internal(Code::INTERNAL, "boom");
        assert!(!err.stacktrace().is_empty());
        let overridden = err.with_stacktrace("custom\n\tfile.rs:1\n");
        assert_eq!(overridden.stacktrace(), "custom\n\tfile.rs:1\n");
    }

    #[test]
    fn is_type_and_is_code() {
        let err = Error::new_not_found(Code::NOT_FOUND, "missing");
        assert!(err.is_type(Type::NotFound));
        assert!(err.is_code(&Code::NOT_FOUND));
        assert!(!err.is_type(Type::Internal));
    }

    #[test]
    fn with_additional_preserves_type_and_code() {
        let inner = Error::new_not_found(Code::NOT_FOUND, "missing");
        let annotated = Error::with_additional(inner, "looked in /etc/config");
        assert!(annotated.is_type(Type::NotFound));
        assert_eq!(annotated.additional().len(), 1);
    }
}
