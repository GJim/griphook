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
            Self::Sink { offset_reset } => Toplevel::new(move |s| async move {
                let result: Result<(), Error> = async {
                    let sinkers = config.binance.sinkers;

                    // TODO: Split sinkers for order of shutdown
                    // let clickhouse_sinkers = sinkers
                    //     .clone()
                    //     .into_iter()
                    //     .filter(|sinker| matches!(sinker.storage, Storage::Clickhouse))
                    //     .collect::<Vec<_>>();

                    // let postgres_sinkers = sinkers
                    //     .into_iter()
                    //     .filter(|sinker| matches!(sinker.storage, Storage::Postgres))
                    //     .collect::<Vec<_>>();

                    let clickhouse_sender = if sinkers
                        .iter()
                        .any(|sinker| matches!(sinker.storage, Storage::Clickhouse))
                    {
                        let (tx, rx) = tokio::sync::mpsc::channel::<DatabaseMessage>(10);
                        let clickhouse_client = config.clickhouse.create_client();
                        let _unused = s.start(SubsystemBuilder::new(
                            "binance-clickhouse",
                            move |h| async move {
                                ClickhouseManager::new_client(clickhouse_client).run(rx, h).await
                            },
                        ));
                        Some(tx)
                    } else {
                        None
                    };

                    let postgres_sender =
                        if sinkers.iter().any(|sinker| matches!(sinker.storage, Storage::Postgres))
                        {
                            let postgres_client =
                                config.postgres.create_pool().await.context(error::ConfigSnafu)?;
                            let (tx, rx) = tokio::sync::mpsc::channel::<DatabaseMessage>(10);
                            let _unused = s.start(SubsystemBuilder::new(
                                "binance-postgres",
                                move |h| async move {
                                    PostgresManager::new_pool(postgres_client)
                                        .run(rx, h)
                                        .await
                                        .context(error::BinanceSnafu)
                                },
                            ));
                            Some(tx)
                        } else {
                            None
                        };

                    for sinker in sinkers {
                        let table_name = sinker.topic.replace('.', "_");
                        match sinker.storage {
                            Storage::Clickhouse => {
                                let consumer = config.kafka.create_consumer(
                                    offset_reset.as_ref(),
                                    Some("clickhouse".to_string()),
                                )?;
                                if let Some(tx) = clickhouse_sender.clone() {
                                    let sink = Sink::new(tx, consumer, table_name);
                                    let _unused = s.start(SubsystemBuilder::new(
                                        format!("binance-{}-sinker", sinker.topic),
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
                                } else {
                                    return Err(Error::StorageNotInitialized {
                                        storage: "clickhouse".to_string(),
                                    });
                                }
                            }
                            Storage::Postgres => {
                                let consumer = config.kafka.create_consumer(
                                    offset_reset.as_ref(),
                                    Some("postgres".to_string()),
                                )?;
                                if let Some(tx) = postgres_sender.clone() {
                                    let sink = Sink::new(tx, consumer, table_name);
                                    let _unused = s.start(SubsystemBuilder::new(
                                        format!("binance-{}-sinker", sinker.topic),
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
                                } else {
                                    return Err(Error::StorageNotInitialized {
                                        storage: "postgres".to_string(),
                                    });
                                }
                            }
                        }
                    }

                    Ok(())
                }
                .await;

                if let Err(e) = result {
                    tracing::error!("Error in Toplevel::new: {}", e);
                }
            })
            .catch_signals()
            .handle_shutdown_requests(Duration::from_secs(5))
            .await
            .context(error::ShutdownTokioRuntimeSnafu),
        }
    }
}
