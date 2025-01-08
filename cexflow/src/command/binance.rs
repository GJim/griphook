use clap::{Subcommand, ValueEnum};
use snafu::ResultExt;
use tokio::time::Duration;
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};

use crate::{
    error::{self, Error},
    Config,
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
    },
}

impl Commands {
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, config: &Config) -> Result<(), Error> {
        match self {
            Self::TradeStream => {
                let producer = config.kafka.create_producer()?;
                let subscription_endpoint = config.binance.subscription_endpoint()?;
                let trading_type = config.binance.trading_type.to_string();

                Toplevel::new(|s| async move {
                    let _unused =
                        s.start(SubsystemBuilder::new("binance-trade-stream", move |h| {
                            griphook_binance::run(trading_type, subscription_endpoint, producer, h)
                        }));
                })
                .catch_signals()
                .handle_shutdown_requests(Duration::from_secs(5))
                .await
                .context(error::ShutdownTokioRuntimeSnafu)
            }
            Self::Inspect { topic, offset_reset } => {
                let consumer = config.kafka.create_consumer(offset_reset.as_ref())?;
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
        }
    }
}

fn extract_stream_type(input: &str) -> Result<String, Error> {
    let parts: Vec<&str> = input.split('.').collect();

    if parts.len() > 2 {
        let stream_type = parts[2].to_lowercase();
        if stream_type.contains("ticker_") {
            return Ok("windowticker".to_string());
        } else if stream_type.contains("depth") && stream_type.len() > 5 {
            return Ok("partialbookdepth".to_string());
        } else if stream_type.contains("continuouskline_") {
            return Ok("continuouskline".to_string());
        } else if stream_type.contains("kline") {
            return Ok("kline".to_string());
        }
        Ok(stream_type)
    } else {
        Err(Error::InvalidStreamTopic { topic: input.to_string() })
    }
}

#[derive(Debug)]
enum StreamType {
    AggTrade,
    AvgPrice,
    BookDepth,
    BookTicker,
    ContinuousKline,
    Kline,
    ForceOrder,
    MarkPrice,
    MiniTicker,
    PartialBookDepth,
    Ticker,
    Trade,
    WindowTicker,
}

impl TryFrom<&String> for StreamType {
    type Error = Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match extract_stream_type(value)?.as_str() {
            "aggtrade" => Ok(Self::AggTrade),
            "trade" => Ok(Self::Trade),
            "avgprice" => Ok(Self::AvgPrice),
            "depth" => Ok(Self::BookDepth),
            "bookticker" => Ok(Self::BookTicker),
            "kline" => Ok(Self::Kline),
            "miniticker" => Ok(Self::MiniTicker),
            "partialbookdepth" => Ok(Self::PartialBookDepth),
            "ticker" => Ok(Self::Ticker),
            "windowticker" => Ok(Self::WindowTicker),
            "forceorder" => Ok(Self::ForceOrder),
            "markprice" => Ok(Self::MarkPrice),
            "continuouskline" => Ok(Self::ContinuousKline),
            _ => Err(Error::InvalidStreamTopic { topic: value.to_string() }),
        }
    }
}
