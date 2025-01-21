use griphook_base::PROJECT_NAME;
use rdkafka::{consumer::StreamConsumer, producer::FutureProducer, ClientConfig};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::ConfigError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaConfig {
    #[serde(default = "KafkaConfig::default_brokers")]
    #[serde(with = "http_serde::uri")]
    pub brokers: http::Uri,

    #[serde(default = "KafkaConfig::default_client_id")]
    pub client_id: String,

    #[serde(default = "KafkaConfig::default_retry")]
    pub retry: u64,

    #[serde(default = "KafkaConfig::default_compression_type")]
    pub compression_type: Option<String>,

    #[serde(default = "KafkaConfig::default_compression_level")]
    pub compression_level: Option<i32>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: Self::default_brokers(),
            client_id: Self::default_client_id(),
            retry: Self::default_retry(),
            compression_type: Self::default_compression_type(),
            compression_level: Self::default_compression_level(),
        }
    }
}

impl KafkaConfig {
    #[inline]
    #[must_use]
    pub fn default_brokers() -> http::Uri {
        "PLAINTEXT://localhost:9092".parse().expect("Failed to parse default brokers")
    }

    #[inline]
    #[must_use]
    pub fn default_client_id() -> String {
        PROJECT_NAME.to_string()
    }

    #[inline]
    #[must_use]
    pub const fn default_retry() -> u64 {
        3
    }

    #[inline]
    #[must_use]
    #[allow(clippy::unnecessary_wraps)]
    pub fn default_compression_type() -> Option<String> {
        Some("zstd".to_string())
    }

    #[inline]
    #[must_use]
    #[allow(clippy::unnecessary_wraps)]
    pub const fn default_compression_level() -> Option<i32> {
        Some(10)
    }

    pub fn create_producer(&self) -> Result<FutureProducer, ConfigError::Error> {
        // Build Kafka producer configuration
        let mut config = ClientConfig::new();
        let config = config
            .set("bootstrap.servers", self.brokers.to_string())
            .set("client.id", &self.client_id)
            .set("retries", self.retry.to_string());

        // Set compression type
        if let Some(compression_type) = &self.compression_type {
            let _unused = config.set("compression.type", compression_type);
        }

        // Set specific compression levels
        if let Some(level) = self.compression_level {
            let _unused = config.set("compression.level", level.to_string());
        }

        config.create().context(ConfigError::KafkaProducerSnafu)
    }

    pub fn create_consumer(
        &self,
        offset_reset: &str,
        client_id: Option<String>,
    ) -> Result<StreamConsumer, ConfigError::Error> {
        let group_id = client_id.unwrap_or_else(|| self.client_id.clone());
        ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", self.brokers.to_string())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", offset_reset)
            .create()
            .context(ConfigError::KafkaConsumerSnafu)
    }
}
