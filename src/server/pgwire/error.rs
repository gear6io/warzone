//! Converts this crate's [`errors::Error`] into a pgwire wire-protocol
//! error, mirroring `errors::as_json` on the HTTP side but targeting
//! Postgres SQLSTATE instead of an HTTP status.

use errors::{sqlstate, Code, Error};
use pgwire::error::{ErrorInfo, PgWireError};

const SEVERITY_ERROR: &str = "ERROR";

pub fn to_pgwire_error(err: &Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        SEVERITY_ERROR.to_string(),
        sqlstate(err.kind()).to_string(),
        err.message().to_string(),
    )))
}

/// The query engine isn't wired up yet — every query fails with this until
/// that lands as a separate task.
pub fn query_engine_not_implemented() -> PgWireError {
    to_pgwire_error(&Error::new_unsupported(
        Code::must_new("query_engine_not_implemented"),
        "pg-wire protocol support is enabled, but query execution is not implemented yet",
    ))
}
