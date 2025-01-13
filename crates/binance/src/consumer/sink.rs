use rdkafka::{
    consumer::{Consumer as KafkaConsumer, StreamConsumer},
    Message,
};
use snafu::ResultExt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
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
    pub async fn run(self, topic: String, subsys: SubsystemHandle) -> Result<()> {
        self.consumer.subscribe(&[&topic]).context(error::KafkaConsumerSnafu)?;

        // create the table if it doesn't exist
        self.db.ensure_table_exists(&self.table_name).await.context(error::ModelSnafu)?;

        tracing::info!("Starting to consume topic {topic} into table {}", self.table_name);

        loop {
            tokio::select! {
                () = subsys.on_shutdown_requested() => {
                    tracing::info!("Shutdown requested");
                    break;
                }
                message_opt = self.consumer.recv() => {
                    if let Some(message) = message_opt.context(error::KafkaConsumerSnafu)?.payload() {
                        let event = T::deserialize_from_avro(message)
                            .context(error::AvroSerializationSnafu)?;

                        tracing::debug!("Received event: {event:?}");
                        let row = self.db.to_row(event).await.context(error::ModelSnafu)?;
                        self.db.insert_row(&self.table_name, row).await.context(error::ModelSnafu)?;
                    }
                }
            }
        }

        Ok(())
    }
}
