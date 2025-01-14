use rdkafka::{
    consumer::{CommitMode, Consumer as KafkaConsumer, StreamConsumer},
    message::BorrowedMessage,
    Message,
};
use snafu::ResultExt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio_graceful_shutdown::SubsystemHandle;

use crate::{
    error::{self, Result},
    models::Avro,
    Database,
};

#[allow(dead_code)]
pub struct Consumer<DB, T, U>
where
    DB: Database<T, U> + Send + Sync,
    T: Avro + Debug + Send + Sync,
    U: Send + Sync,
{
    db: Arc<DB>,
    consumer: StreamConsumer,
    table_name: String,
    _phantom: PhantomData<(T, U)>,
}

#[allow(dead_code)]
impl<DB, T, U> Consumer<DB, T, U>
where
    DB: Database<T, U> + Send + Sync,
    T: Avro + Debug + Send + Sync,
    U: Send + Sync,
{
    #[must_use]
    pub const fn new(db: Arc<DB>, consumer: StreamConsumer, table_name: String) -> Self {
        Self { db, consumer, table_name, _phantom: PhantomData }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn run(
        self,
        topic: String,
        batch_size: usize,
        batch_timeout: Duration,
        subsys: SubsystemHandle,
    ) -> Result<()> {
        self.consumer.subscribe(&[&topic]).context(error::KafkaConsumerSnafu)?;

        // create the table if it doesn't exist
        self.db.ensure_table_exists(&self.table_name).await.context(error::ModelSnafu)?;

        tracing::info!(
            "Starting to consume topic {topic} into table {} with batch size {} and timeout {:?}",
            self.table_name,
            batch_size,
            batch_timeout
        );

        let mut messages = Vec::with_capacity(batch_size);

        loop {
            tokio::select! {
                () = subsys.on_shutdown_requested() => {
                    tracing::info!("Shutdown requested");
                    // Process remaining batch if any
                    if !messages.is_empty() {
                        self.process_batch(&messages).await?;
                    }
                    break;
                }
                message_opt = tokio::time::timeout(batch_timeout, self.consumer.recv()) => {
                    match message_opt {
                        Ok(Ok(message)) => {
                            messages.push(message);

                            if messages.len() >= batch_size {
                                self.process_batch(&messages).await?;
                                messages.clear();
                            }
                        }
                        Ok(Err(e)) => {
                            return Err(e).context(error::KafkaConsumerSnafu);
                        }
                        Err(_) => {
                            // Timeout occurred, process current batch if any
                            if !messages.is_empty() {
                                self.process_batch(&messages).await?;
                                messages.clear();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_batch(&self, messages: &[BorrowedMessage<'_>]) -> Result<()> {
        // Convert events to rows
        let mut rows = Vec::with_capacity(messages.len());
        for message in messages {
            if let Some(payload) = message.payload() {
                let event =
                    T::deserialize_from_avro(payload).context(error::AvroSerializationSnafu)?;

                let row = self.db.to_row(event).await.context(error::ModelSnafu)?;
                rows.push(row);
            }
        }

        self.db.insert_row_batch(&self.table_name, rows).await.context(error::ModelSnafu)?;

        // Commit all messages in the batch
        if let Some(last_message) = messages.last() {
            self.consumer
                .commit_message(last_message, CommitMode::Async)
                .context(error::KafkaConsumerSnafu)?;
        }

        tracing::debug!("Processed batch of {} messages", messages.len());
        Ok(())
    }
}
