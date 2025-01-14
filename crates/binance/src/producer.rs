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

async fn connect_websocket(
    subscription_endpoint: &http::Uri,
) -> Result<(
    futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
)> {
    let (ws_stream, _) =
        connect_async(subscription_endpoint).await.context(WebSocketConnectionSnafu)?;
    Ok(ws_stream.split())
}

async fn handle_websocket_error(
    retry_count: &mut u32,
    backoff_duration: &mut Duration,
    max_retries: u32,
    error: crate::error::Error,
    context: &str,
) -> Result<()> {
    if *retry_count >= max_retries {
        tracing::error!("Max retries reached. {context}");
        return Err(error);
    }
    *retry_count += 1;
    tracing::warn!(
        "WebSocket {context} (attempt {retry_count}/{max_retries}), retrying in {backoff_duration:?}: {error:?}",
    );
    tokio::time::sleep(*backoff_duration).await;
    *backoff_duration *= 2; // Exponential backoff
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
#[allow(clippy::too_many_lines)]
pub async fn run(
    trading_type: String,
    subscription_endpoint: http::Uri,
    producer: FutureProducer,
    subsys: SubsystemHandle,
) -> Result<()> {
    let max_retries = 3;
    let mut retry_count = 0;
    let mut backoff_duration = Duration::from_secs(1);

    loop {
        tracing::info!("Connecting to {}", subscription_endpoint);

        let (mut write, mut read) = match connect_websocket(&subscription_endpoint).await {
            Ok(connection) => {
                retry_count = 0;
                backoff_duration = Duration::from_secs(1);
                connection
            }
            Err(e) => {
                handle_websocket_error(
                    &mut retry_count,
                    &mut backoff_duration,
                    max_retries,
                    e,
                    "connection failed",
                )
                .await?;
                continue;
            }
        };

        let ws_loop_result: Result<()> = async {
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
                                    return Ok(());
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
                            return Ok(());
                        }
                    }
                    () = subsys.on_shutdown_requested() => {
                        tracing::info!("Shutdown requested");
                        let _unused = write.send(Message::Close(None)).await;
                        return Ok(());
                    }
                }
            }
        }
        .await;

        match ws_loop_result {
            Ok(()) => {
                if subsys.is_shutdown_requested() {
                    break;
                }
                tracing::warn!("WebSocket connection closed normally, attempting to reconnect");
                continue;
            }
            Err(e) => {
                handle_websocket_error(
                    &mut retry_count,
                    &mut backoff_duration,
                    max_retries,
                    e,
                    "error occurred",
                )
                .await?;
                continue;
            }
        }
    }

    Ok(())
}
