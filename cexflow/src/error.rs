use snafu::Snafu;

use crate::config;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{source}"))]
    Config { source: config::Error },

    #[snafu(display("{source}"))]
    Binance { source: griphook_binance::Error },

    #[snafu(display("Failed to initialize tokio runtime: {source}"))]
    InitializeTokioRuntime { source: tokio::io::Error },

    #[snafu(display("Failed to shutdown tokio runtime: {source}"))]
    ShutdownTokioRuntime { source: tokio_graceful_shutdown::errors::GracefulShutdownError },

    #[snafu(display("Failed to create Kafka consumer: {source}"))]
    CreateKafkaConsumer { source: rdkafka::error::KafkaError },

    #[snafu(display("Failed to subscribe to topic: {source}"))]
    SubscribeTopic { source: rdkafka::error::KafkaError },

    #[snafu(display("Invalid stream topic: {topic}"))]
    InvalidStreamTopic { topic: String },

    #[snafu(display("Unsupported stream type: {stream_type}"))]
    UnsupportedStreamType { stream_type: String },

    #[snafu(display("Unsupported storage: {storage}"))]
    UnsupportedStorage { storage: String },

    #[snafu(display("{storage} is not initialize successfully"))]
    StorageNotInitialized { storage: String },
}

impl From<config::Error> for Error {
    fn from(source: config::Error) -> Self {
        Self::Config { source }
    }
}

impl From<griphook_binance::Error> for Error {
    fn from(source: griphook_binance::Error) -> Self {
        match source {
            griphook_binance::Error::InvalidStreamTopic { topic } => {
                Self::InvalidStreamTopic { topic }
            }
            _ => Self::Binance { source },
        }
    }
}

impl From<tokio_graceful_shutdown::errors::GracefulShutdownError> for Error {
    fn from(source: tokio_graceful_shutdown::errors::GracefulShutdownError) -> Self {
        Self::ShutdownTokioRuntime { source }
    }
}

pub trait CommandError {
    fn exit_code(&self) -> exitcode::ExitCode;
}

impl CommandError for Error {
    fn exit_code(&self) -> exitcode::ExitCode {
        match self {
            Self::Config { .. }
            | Self::InvalidStreamTopic { .. }
            | Self::UnsupportedStreamType { .. }
            | Self::UnsupportedStorage { .. }
            | Self::StorageNotInitialized { .. } => exitcode::CONFIG,
            Self::Binance { .. } => exitcode::SOFTWARE,
            Self::InitializeTokioRuntime { .. } | Self::ShutdownTokioRuntime { .. } => {
                exitcode::IOERR
            }
            Self::CreateKafkaConsumer { .. } | Self::SubscribeTopic { .. } => exitcode::UNAVAILABLE,
        }
    }
}
