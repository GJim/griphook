use clap::{Subcommand, ValueEnum};
use snafu::ResultExt;
use tokio::time::Duration;
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};

use crate::{
    config::Storage,
    error::{self, Error},
    Config,
};

use griphook_binance::{
    database::{
        ClickhouseManager, DatabaseManager, DatabaseMessage, DatabaseType, PostgresManager,
    },
    Sink, StreamType,
};

#[derive(Debug, Clone, ValueEnum)]
pub enum OffsetReset {
    Earliest,
    Latest,
}

impl AsRef<str> for OffsetReset {
    fn as_ref(&self) -> &str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }
}

#[derive(Clone, Subcommand)]
pub enum Commands {
    #[command(about = "Subscribe to binance trade stream and push to kafka.")]
    TradeStream,

    #[command(about = "Inspect messages from a specific Kafka topic")]
    Inspect {
        #[arg(help = "The Kafka topic to inspect")]
        topic: String,

        #[clap(
            long = "offset",
            short = 'o',
            help = "Specify the offset reset value",
            default_value = "latest",
            value_enum
        )]
        offset_reset: OffsetReset,

        #[clap(long = "client-id", short = 'c', help = "The Kafka client id to use")]
        client_id: Option<String>,
    },

    #[command(about = "Push messages from a specific Kafka topic to storage")]
    Sink {
        #[clap(
            long = "offset",
            short = 'o',
            help = "Specify the offset reset value",
            default_value = "latest",
            value_enum
        )]
        offset_reset: OffsetReset,
    },
}

impl Commands {
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, config: Config) -> Result<(), Error> {
        match self {
            Self::TradeStream => {
                let producer = config.kafka.create_producer()?;
                let subscription_endpoint = config.binance.subscription_endpoint()?;
                let trading_type = config.binance.trading_type.to_string();

                Toplevel::new(|s| async move {
                    let _unused =
                        s.start(SubsystemBuilder::new("binance-trade-stream", move |h| {
                            griphook_binance::producer::run(
                                trading_type,
                                subscription_endpoint,
                                producer,
                                h,
                            )
                        }));
                })
                .catch_signals()
                .handle_shutdown_requests(Duration::from_secs(5))
                .await
                .context(error::ShutdownTokioRuntimeSnafu)
            }
            Self::Inspect { topic, offset_reset, client_id } => {
                let consumer = config.kafka.create_consumer(offset_reset.as_ref(), client_id)?;
                let stream_type = StreamType::try_from(&topic)?;
                tracing::info!("Starting to inspect stream type: {stream_type:?}");
                Toplevel::new(|s| async move {
                    let _unused = match stream_type {
                        StreamType::AggTrade => {
                            s.start(SubsystemBuilder::new("binance-aggtrade-inspector", move |h| {
                                griphook_binance::inspector::run::<griphook_binance::AggTrade>(
                                    consumer, topic, h,
                                )
                            }))
                        }
                        StreamType::AvgPrice => {
                            s.start(SubsystemBuilder::new("binance-avgprice-inspector", move |h| {
                                griphook_binance::inspector::run::<griphook_binance::AvgPrice>(
                                    consumer, topic, h,
                                )
                            }))
                        }
                        StreamType::BookDepth => s.start(SubsystemBuilder::new(
                            "binance-bookdepth-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::BookDepth>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::BookTicker => s.start(SubsystemBuilder::new(
                            "binance-bookticker-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::BookTicker>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::Kline => {
                            s.start(SubsystemBuilder::new("binance-kline-inspector", move |h| {
                                griphook_binance::inspector::run::<griphook_binance::Kline>(
                                    consumer, topic, h,
                                )
                            }))
                        }
                        StreamType::MiniTicker => s.start(SubsystemBuilder::new(
                            "binance-miniticker-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::MiniTicker>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::PartialBookDepth => {
                            s.start(SubsystemBuilder::new(
                                "binance-partialbookdepth-inspector",
                                move |h| {
                                    griphook_binance::inspector::run::<
                                        griphook_binance::PartialBookDepth,
                                    >(consumer, topic, h)
                                },
                            ))
                        }
                        StreamType::Ticker => {
                            s.start(SubsystemBuilder::new("binance-ticker-inspector", move |h| {
                                griphook_binance::inspector::run::<griphook_binance::Ticker>(
                                    consumer, topic, h,
                                )
                            }))
                        }
                        StreamType::Trade => {
                            s.start(SubsystemBuilder::new("binance-trade-inspector", move |h| {
                                griphook_binance::inspector::run::<griphook_binance::Trade>(
                                    consumer, topic, h,
                                )
                            }))
                        }
                        StreamType::WindowTicker => s.start(SubsystemBuilder::new(
                            "binance-windowticker-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::WindowTicker>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::ForceOrder => s.start(SubsystemBuilder::new(
                            "binance-forceorder-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::ForceOrder>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::MarkPrice => s.start(SubsystemBuilder::new(
                            "binance-markprice-inspector",
                            move |h| {
                                griphook_binance::inspector::run::<griphook_binance::MarkPrice>(
                                    consumer, topic, h,
                                )
                            },
                        )),
                        StreamType::ContinuousKline => {
                            s.start(SubsystemBuilder::new(
                                "binance-continuouskline-inspector",
                                move |h| {
                                    griphook_binance::inspector::run::<
                                        griphook_binance::ContinuousKline,
                                    >(consumer, topic, h)
                                },
                            ))
                        }
                    };
                })
                .catch_signals()
                .handle_shutdown_requests(Duration::from_secs(5))
                .await
                .context(error::ShutdownTokioRuntimeSnafu)
            }
            Self::Sink { offset_reset } => Toplevel::new(|s| async move {
                let sinkers = config.binance.sinkers;

                // Split sinkers by storage type
                let clickhouse_sinkers = sinkers
                    .clone()
                    .into_iter()
                    .filter(|sinker| matches!(sinker.storage, Storage::Clickhouse))
                    .collect::<Vec<_>>();

                let postgres_sinkers = sinkers
                    .into_iter()
                    .filter(|sinker| matches!(sinker.storage, Storage::Postgres))
                    .collect::<Vec<_>>();

                // Initialize ClickHouse subsystem if needed
                if !clickhouse_sinkers.is_empty() {
                    let (tx, rx) = tokio::sync::mpsc::channel::<DatabaseMessage>(10);
                    let clickhouse_client = config.clickhouse.create_client();

                    // Start ClickHouse manager as parent subsystem
                    let kafka_config = config.kafka.clone();
                    let kafka_offset_reset = offset_reset.clone();
                    let _unused = s.start(SubsystemBuilder::new(
                        "binance-clickhouse-manager",
                        move |clickhouse_handle| async move {
                            // Start individual ClickHouse sinkers as child subsystems
                            for sinker in clickhouse_sinkers {
                                let table_name = sinker.topic.replace('.', "_");
                                let consumer = kafka_config
                                    .create_consumer(
                                        kafka_offset_reset.as_ref(),
                                        Some("clickhouse".to_string()),
                                    )
                                    .context(error::ConfigSnafu)?;

                                let sink = Sink::new(tx.clone(), consumer, table_name);
                                let _unused = clickhouse_handle.start(SubsystemBuilder::new(
                                    format!("binance-{}-clickhouse-sinker", sinker.topic),
                                    move |h| {
                                        sink.run(
                                            sinker.topic,
                                            DatabaseType::from(sinker.storage),
                                            sinker.batch_size,
                                            Duration::from_secs(sinker.batch_timeout),
                                            h,
                                        )
                                    },
                                ));
                            }
                            ClickhouseManager::new_client(clickhouse_client)
                                .run(rx, clickhouse_handle)
                                .await
                                .context(error::BinanceSnafu)
                        },
                    ));
                }

                // Initialize Postgres subsystem if needed
                if !postgres_sinkers.is_empty() {
                    let (tx, rx) = tokio::sync::mpsc::channel::<DatabaseMessage>(10);

                    // Start Postgres manager as parent subsystem
                    let _unused = s.start(SubsystemBuilder::new(
                        "binance-postgres-manager",
                        move |postgres_handle| async move {
                            // Start individual Postgres sinkers as child subsystems
                            for sinker in postgres_sinkers {
                                let table_name = sinker.topic.replace('.', "_");
                                let consumer = config.kafka.create_consumer(
                                    offset_reset.as_ref(),
                                    Some("postgres".to_string()),
                                )?;

                                let sink = Sink::new(tx.clone(), consumer, table_name);
                                let _unused = postgres_handle.start(SubsystemBuilder::new(
                                    format!("binance-{}-postgres-sinker", sinker.topic),
                                    move |h| {
                                        sink.run(
                                            sinker.topic,
                                            DatabaseType::from(sinker.storage),
                                            sinker.batch_size,
                                            Duration::from_secs(sinker.batch_timeout),
                                            h,
                                        )
                                    },
                                ));
                            }
                            let postgres_client =
                                config.postgres.create_pool().await.context(error::ConfigSnafu)?;
                            PostgresManager::new_pool(postgres_client)
                                .run(rx, postgres_handle)
                                .await
                                .context(error::BinanceSnafu)
                        },
                    ));
                }
            })
            .catch_signals()
            .handle_shutdown_requests(Duration::from_secs(5))
            .await
            .context(error::ShutdownTokioRuntimeSnafu),
        }
    }
}
