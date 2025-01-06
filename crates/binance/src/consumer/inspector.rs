use std::fmt::Debug;

use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    Message,
};
use snafu::ResultExt;
use tokio_graceful_shutdown::SubsystemHandle;

use crate::{
    error::{self, Result},
    models::Avro,
};

#[allow(clippy::missing_errors_doc)]
pub async fn run<T: Avro + Debug>(
    consumer: StreamConsumer,
    topic: String,
    subsys: SubsystemHandle,
) -> Result<()> {
    consumer.subscribe(&[&topic]).context(error::KafkaConsumerSnafu)?;

    tracing::info!("Starting to inspect topic: {}", topic);

    loop {
        tokio::select! {
            () = subsys.on_shutdown_requested() => {
                tracing::info!("Shutdown requested");
                break;
            }
            message_opt = consumer.recv() => {
                if let Some(message) = message_opt.context(error::KafkaConsumerSnafu)?.payload() {
                    let event = T::deserialize_from_avro(message)
                        .context(error::AvroSerializationSnafu)?;
                    tracing::info!("Received event: {event:?}");
                }
            }
        }
    }

    Ok(())
}
