pub mod backend;
pub mod config;
pub mod destination;
pub mod ingest;

/// Wraps an iceberg-rust error at the crate boundary, per `errors`' contract
/// that only the boundary receiving an untyped external error assigns it a
/// type/code/message. Callers pass their own `Code` so distinct failure
/// sites stay distinguishable instead of collapsing onto one generic code.
pub(crate) fn wrap_iceberg(
    e: iceberg::Error,
    code: errors::Code,
    message: impl Into<String>,
) -> errors::Error {
    errors::Error::wrap_internal(e, code, message)
}

/// Identifies one logical stream/table synced into a destination.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub namespace: Vec<String>,
    pub table: String,
}

impl StreamId {
    pub fn new(
        namespace: impl IntoIterator<Item = impl Into<String>>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into_iter().map(Into::into).collect(),
            table: table.into(),
        }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in &self.namespace {
            write!(f, "{part}.")?;
        }
        write!(f, "{}", self.table)
    }
}
