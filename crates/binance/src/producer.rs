use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rdkafka::producer::{FutureProducer, FutureRecord};
use snafu::ResultExt;
use tokio_graceful_shutdown::SubsystemHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    error::{
        KafkaProducerSnafu, MessageParseSnafu, Result, WebSocketConnectionSnafu,
        WebSocketMessageSnafu,
    },
    EventMessage,
};

#[allow(clippy::missing_errors_doc)]
pub async fn run(
    trading_type: String,
    subscription_endpoint: http::Uri,
    producer: FutureProducer,
    subsys: SubsystemHandle,
) -> Result<()> {
    tracing::info!("Connecting to {}", subscription_endpoint);

    let (ws_stream, _) =
        connect_async(subscription_endpoint).await.context(WebSocketConnectionSnafu)?;
    let (mut write, mut read) = ws_stream.split();

    loop {
        tokio::select! {
            msg_opt = read.next() => {
                if let Some(msg) = msg_opt {
                    let msg = msg.context(WebSocketMessageSnafu)?;
                    tracing::debug!("Received message: {msg:?}");
                    match msg {
                        Message::Text(_) | Message::Binary(_) => {
                            let data = msg.into_data();
                            let event_message: EventMessage =
                                serde_json::from_slice(&data).context(MessageParseSnafu)?;

                            tracing::debug!("Deserialized message: {event_message:?}");

                            let topic = event_message.topic(&trading_type)?;
                            let sequence_id = event_message.data.sequence_id();
                            let payload = event_message.data.to_compressed_message()?;

                            let record = FutureRecord::to(&topic).payload(&payload).key(&sequence_id);

                            let _unused = producer
                                .send(record, Duration::from_secs(0))
                                .await
                                .map_err(|(e, _)| e)
                                .context(KafkaProducerSnafu)?;
                        }
                        Message::Close(frame) => {
                            tracing::info!("Received close frame: {frame:?}");
                            if let Some(frame) = frame {
                                let _unused = write.send(Message::Close(Some(frame))).await;
                            }
                            break;
                        }
                        Message::Ping(payload) => {
                            tracing::info!("Received ping frame: {payload:?}");
                            let _unused = write.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(payload) => {
                            tracing::info!("Received pong frame: {payload:?}");
                        }
                        Message::Frame(_) => {}
                    }
                } else {
                    tracing::info!("WebSocket stream ended");
                    let _unused = write.send(Message::Close(None)).await;
                    break;
                }
            }
            () = subsys.on_shutdown_requested() => {
                tracing::info!("Shutdown requested");
                let _unused = write.send(Message::Close(None)).await;
                break;
            }
        }
    }

    Ok(())
}
