//! Converts this crate's [`errors::Error`] into a pgwire wire-protocol
//! error, mirroring `errors::as_json` on the HTTP side but targeting
//! Postgres SQLSTATE instead of an HTTP status.

use errors::{sqlstate, Error};
use pgwire::error::{ErrorInfo, PgWireError};

const SEVERITY_ERROR: &str = "ERROR";

pub fn to_pgwire_error(err: &Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        SEVERITY_ERROR.to_string(),
        sqlstate(err.kind()).to_string(),
        err.message().to_string(),
    )))
}
