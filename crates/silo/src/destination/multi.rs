use std::sync::LazyLock;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::Schema as IcebergSchema;
use tokio::task::JoinSet;

use errors::{Code, Error};

use super::Destination;
use crate::backend::DestinationWriter;
use crate::StreamId;

static CODE_DESTINATION_TASK_PANICKED: LazyLock<Code> = LazyLock::new(|| Code::must_new("destination_task_panicked"));
static CODE_DESTINATIONS_FAILED: LazyLock<Code> = LazyLock::new(|| Code::must_new("destinations_failed"));
/// Fans the same call out to every configured destination writer.
///
/// **Fail-fast, without cancellation.** All destinations for the *current*
/// call are dispatched concurrently and run to completion — an in-flight
/// Iceberg transaction commit can't be safely cancelled mid-request (the
/// REST/catalog server may still apply it), so aborting a task would only
/// destroy our reference to that destination's writer state while possibly
/// not stopping the commit at all. "Fail-fast" instead means: any failure is
/// surfaced immediately as `Err` (never silently swallowed), which — via
/// ordinary `?` propagation up through [`crate::ingest::TableSink`] — stops
/// the caller from issuing *further* batches to *any* destination once one
/// has failed. It does **not** mean an already-committed destination gets
/// rolled back: there is no cross-catalog atomic transaction, so a failure
/// in destination B never undoes destination A's independent commit.
pub struct MultiDestination {
    writers: Vec<Box<dyn DestinationWriter>>,
}

impl MultiDestination {
    pub fn new(writers: Vec<Box<dyn DestinationWriter>>) -> Self {
        Self { writers }
    }
}

async fn join_all(
    mut set: JoinSet<(Box<dyn DestinationWriter>, Result<(), Error>)>,
) -> (Vec<Box<dyn DestinationWriter>>, Vec<(String, Error)>) {
    let mut writers = Vec::new();
    let mut failures = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((writer, Ok(()))) => writers.push(writer),
            Ok((writer, Err(err))) => {
                failures.push((writer.name().to_string(), err));
                writers.push(writer);
            }
            Err(join_err) => failures.push((
                "<task panicked>".to_string(),
                Error::wrap_internal(join_err, CODE_DESTINATION_TASK_PANICKED.clone(), "destination task panicked"),
            )),
        }
    }
    (writers, failures)
}

/// One or more destinations failed during a fan-out operation. `failures`
/// pairs each failing destination's name with its error; destinations not
/// listed here succeeded (and, per `MultiDestination`'s fail-fast contract,
/// may have already committed independently).
fn finish(total: usize, failures: Vec<(String, Error)>) -> Result<(), Error> {
    if failures.is_empty() {
        Ok(())
    } else {
        let succeeded = total - failures.len();
        let detail = failures
            .iter()
            .map(|(name, err)| format!("{name}: {err}"))
            .collect::<Vec<_>>()
            .join("; ");
        Err(Error::new_internal(
            CODE_DESTINATIONS_FAILED.clone(),
            format!("{} of {} destinations failed: {detail}", failures.len(), failures.len() + succeeded),
        ))
    }
}

#[async_trait]
impl Destination for MultiDestination {
    async fn ensure_table(&mut self, stream: &StreamId, schema: &IcebergSchema) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for mut writer in self.writers.drain(..) {
            let stream = stream.clone();
            let schema = schema.clone();
            set.spawn(async move {
                let res = writer.ensure_table(&stream, &schema).await.map(|_| ());
                (writer, res)
            });
        }
        let (writers, failures) = join_all(set).await;
        let total = writers.len();
        self.writers = writers;
        finish(total, failures)
    }

    async fn write(&mut self, stream: &StreamId, batch: &RecordBatch) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for mut writer in self.writers.drain(..) {
            let stream = stream.clone();
            let batch = batch.clone();
            set.spawn(async move {
                let res = writer.write(&stream, &batch).await;
                (writer, res)
            });
        }
        let (writers, failures) = join_all(set).await;
        let total = writers.len();
        self.writers = writers;
        finish(total, failures)
    }

    async fn evolve_schema(&mut self, stream: &StreamId, new_schema: &IcebergSchema) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for mut writer in self.writers.drain(..) {
            let stream = stream.clone();
            let new_schema = new_schema.clone();
            set.spawn(async move {
                let res = writer.evolve_schema(&stream, &new_schema).await.map(|_| ());
                (writer, res)
            });
        }
        let (writers, failures) = join_all(set).await;
        let total = writers.len();
        self.writers = writers;
        finish(total, failures)
    }

    async fn close(&mut self, stream: &StreamId) -> Result<(), Error> {
        let mut set = JoinSet::new();
        for mut writer in self.writers.drain(..) {
            let stream = stream.clone();
            set.spawn(async move {
                let res = writer.close(&stream).await;
                (writer, res)
            });
        }
        let (writers, failures) = join_all(set).await;
        let total = writers.len();
        self.writers = writers;
        finish(total, failures)
    }
}
