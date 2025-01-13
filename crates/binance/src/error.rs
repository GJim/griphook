use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse WebSocket message: {source}, location: {location}"))]
    MessageParse {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize to Avro: {source}, location: {location}"))]
    AvroSerialization {
        source: avro_rs::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to compress data: {source}, location: {location}"))]
    Compression {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("WebSocket connection error: {source}, location: {location}"))]
    WebSocketConnection {
        source: tokio_tungstenite::tungstenite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("WebSocket message error: {source}, location: {location}"))]
    WebSocketMessage {
        source: tokio_tungstenite::tungstenite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Kafka producer error: {source}, location: {location}"))]
    KafkaProducer {
        source: rdkafka::error::KafkaError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Kafka consumer error: {source}, location: {location}"))]
    KafkaConsumer {
        source: rdkafka::error::KafkaError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported stream type: {stream_type}"))]
    UnsupportedStreamType { stream_type: String },

    #[snafu(display("ClickHouse error: {source}, location: {location}"))]
    Clickhouse {
        source: clickhouse::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("PostgreSQL error: {source}, location: {location}"))]
    Postgres {
        source: sqlx::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Model error: {source}, location: {location}"))]
    Model {
        source: crate::models::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}
