use rdkafka::{
    consumer::{CommitMode, Consumer as KafkaConsumer, StreamConsumer},
    message::BorrowedMessage,
    Message,
};
use snafu::ResultExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::SubsystemHandle;

use crate::{
    database::{
        ClickhouseMessage, ClickhouseRows, DatabaseMessage, DatabaseType, PostgresMessage,
        PostgresRows,
    },
    error::{self, Result},
    models::Avro,
    StreamType,
};

pub struct Sink {
    db: mpsc::Sender<DatabaseMessage>,
    consumer: StreamConsumer,
    table_name: String,
}

impl Sink {
    #[must_use]
    pub const fn new(
        db: mpsc::Sender<DatabaseMessage>,
        consumer: StreamConsumer,
        table_name: String,
    ) -> Self {
        Self { db, consumer, table_name }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn run(
        self,
        topic: String,
        database_type: DatabaseType,
        batch_size: usize,
        batch_timeout: Duration,
        subsys: SubsystemHandle,
    ) -> Result<()> {
        self.consumer.subscribe(&[&topic]).context(error::KafkaConsumerSnafu)?;

        tracing::info!(
            "Starting to consume topic {topic} into table {} with batch size {} and timeout {:?}",
            self.table_name,
            batch_size,
            batch_timeout
        );

        let stream_type = StreamType::try_from(&topic)?;

        let mut messages = Vec::with_capacity(batch_size);

        loop {
            tokio::select! {
                () = subsys.on_shutdown_requested() => {
                    tracing::info!("Sink service shutdown requested");
                    // Process remaining batch if any
                    if !messages.is_empty() {
                        self.process_batch(&stream_type, &database_type, &messages).await?;
                    }
                    tracing::info!("Sink service shutdown completed");
                    break;
                }
                message_opt = tokio::time::timeout(batch_timeout, self.consumer.recv()) => {
                    match message_opt {
                        Ok(Ok(message)) => {
                            messages.push(message);

                            if messages.len() >= batch_size {
                                self.process_batch(&stream_type, &database_type, &messages).await?;
                                messages.clear();
                            }
                        }
                        Ok(Err(e)) => {
                            return Err(e).context(error::KafkaConsumerSnafu);
                        }
                        Err(_) => {
                            // Timeout occurred, process current batch if any
                            if !messages.is_empty() {
                                self.process_batch(&stream_type, &database_type, &messages).await?;
                                messages.clear();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn process_batch(
        &self,
        stream_type: &StreamType,
        database_type: &DatabaseType,
        messages: &[BorrowedMessage<'_>],
    ) -> Result<()> {
        let event = match (stream_type, database_type) {
            (StreamType::AggTrade, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::AggTrade(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::AggTrade::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::AggTradeRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::AvgPrice, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::AvgPrice(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::AvgPrice::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::AvgPriceRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::BookDepth, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::BookDepth(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::BookDepth::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::BookDepthNestedRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::BookTicker, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::BookTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::BookTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::BookTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::ContinuousKline, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::ContinuousKline(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::ContinuousKline::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::ContinuousKlineRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::ForceOrder, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::ForceOrder(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::ForceOrder::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::ForceOrderRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Kline, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::Kline(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Kline::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::KlineRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::MarkPrice, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::MarkPrice(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::MarkPrice::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::MarkPriceRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::MiniTicker, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::MiniTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::MiniTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::MiniTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::PartialBookDepth, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::PartialBookDepth(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::PartialBookDepth::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::PartialBookDepthNestedRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Ticker, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::Ticker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Ticker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::TickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Trade, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::Trade(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Trade::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::TradeRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::WindowTicker, DatabaseType::Clickhouse) => {
                DatabaseMessage::Clickhouse(ClickhouseMessage {
                    table_name: self.table_name.clone(),
                    rows: ClickhouseRows::WindowTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::WindowTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::WindowTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::AggTrade, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::AggTrade(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::AggTrade::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::AggTradeRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::AvgPrice, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::AvgPrice(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::AvgPrice::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::AvgPriceRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::BookDepth, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::BookDepth(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::BookDepth::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::BookDepthRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::BookTicker, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::BookTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::BookTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::BookTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::ContinuousKline, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::ContinuousKline(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::ContinuousKline::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::ContinuousKlineRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::ForceOrder, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::ForceOrder(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::ForceOrder::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::ForceOrderRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Kline, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::Kline(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Kline::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::KlineRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::MarkPrice, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::MarkPrice(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::MarkPrice::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::MarkPriceRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::MiniTicker, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::MiniTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::MiniTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::MiniTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::PartialBookDepth, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::PartialBookDepth(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::PartialBookDepth::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::PartialBookDepthRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Ticker, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::Ticker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Ticker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::TickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::Trade, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::Trade(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::Trade::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::TradeRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
            (StreamType::WindowTicker, DatabaseType::Postgres) => {
                DatabaseMessage::Postgres(PostgresMessage {
                    table_name: self.table_name.clone(),
                    rows: PostgresRows::WindowTicker(
                        messages
                            .iter()
                            .filter_map(|m| {
                                m.payload().and_then(|b| {
                                    crate::WindowTicker::deserialize_from_avro(b)
                                        .context(error::AvroSerializationSnafu)
                                        .and_then(|trade| {
                                            crate::WindowTickerRow::try_from(trade)
                                                .context(error::ModelSnafu)
                                        })
                                        .inspect_err(|e| {
                                            tracing::warn!("Batch processing message: {}", e);
                                        })
                                        .ok()
                                })
                            })
                            .collect(),
                    ),
                })
            }
        };

        self.db.send(event).await.map_err(|_| error::Error::SinkOperation {})?;

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
