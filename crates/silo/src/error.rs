use thiserror::Error;

use crate::StreamId;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("destination '{name}' failed: {source}")]
    Destination {
        name: String,
        #[source]
        source: Box<SinkError>,
    },

    /// iceberg-rust 0.9.1 exposes no public way to evolve an existing table's
    /// schema: `Transaction` has no `update_schema`, and `TableCommit`'s
    /// builder is private ("dangerous and error-prone to construct
    /// directly"), so external crates cannot apply `TableUpdate::AddSchema`
    /// themselves. Revisit once upstream adds a `Transaction` schema action.
    #[error(
        "schema evolution for stream {stream:?} is not supported by iceberg-rust 0.9.1 (no public Transaction::update_schema); new/changed fields: {fields:?}"
    )]
    SchemaEvolutionUnsupported { stream: StreamId, fields: Vec<String> },

    /// One or more destinations failed during a fan-out operation. Each
    /// entry is the failing destination's name paired with its error;
    /// destinations not listed here succeeded (and, per `MultiDestination`'s
    /// fail-fast contract, may have already committed independently).
    #[error("{} of {} destinations failed: {failures:?}", failures.len(), failures.len() + succeeded)]
    MultiDestination {
        failures: Vec<(String, SinkError)>,
        succeeded: usize,
    },

    #[error("unknown stream {0:?}")]
    UnknownStream(StreamId),

    #[error("{0}")]
    Other(String),
}
