use std::path::PathBuf;

use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Could not open config from {filename:?}, error: {source}"))]
    OpenConfig { filename: PathBuf, source: std::io::Error },

    #[snafu(display("Could not parse config from {filename:?}, error: {source}"))]
    ParseConfig { filename: PathBuf, source: serde_yaml::Error },

    #[snafu(display("Could not resolve file path {file_path:?}, error: {source}"))]
    ResolveFilePath { file_path: PathBuf, source: std::io::Error },

    #[snafu(display("Failed to create Kafka producer: {source}"))]
    KafkaProducer { source: rdkafka::error::KafkaError },

    #[snafu(display("Failed to create Kafka consumer: {source}"))]
    KafkaConsumer { source: rdkafka::error::KafkaError },

    #[snafu(display("Invalid offset reset value. Must be either 'earliest' or 'latest'"))]
    InvalidOffsetReset,

    #[snafu(display(
        "Failed to parse binance stream endpoint uri: {source}, uri: {uri}, location: {location}"
    ))]
    ParseBinanceUri {
        source: http::uri::InvalidUri,
        uri: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl From<rdkafka::error::KafkaError> for Error {
    fn from(err: rdkafka::error::KafkaError) -> Self { Self::KafkaProducer { source: err } }
}
