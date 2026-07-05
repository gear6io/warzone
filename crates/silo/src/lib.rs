pub mod backend;
pub mod config;
pub mod destination;
mod error;
pub mod ingest;

pub use error::SinkError;

/// Identifies one logical stream/table synced into a destination.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub namespace: Vec<String>,
    pub table: String,
}

impl StreamId {
    pub fn new(namespace: impl IntoIterator<Item = impl Into<String>>, table: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into_iter().map(Into::into).collect(),
            table: table.into(),
        }
    }
}
