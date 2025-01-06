use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::ConfigError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BinanceConfig {
    #[serde(default = "BinanceConfig::default_stream_endpoint")]
    #[serde(with = "http_serde::uri")]
    pub stream_endpoint: http::Uri,

    pub subscription: Vec<String>,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            stream_endpoint: Self::test_stream_endpoint(),
            subscription: Self::default_subscription(),
        }
    }
}

impl BinanceConfig {
    #[allow(dead_code)]
    fn test_stream_endpoint() -> http::Uri {
        "wss://testnet.binance.vision"
            .parse()
            .expect("Failed to parse binance test stream endpoint")
    }

    #[allow(dead_code)]
    fn default_stream_endpoint() -> http::Uri {
        "wss://stream.binance.com:9443".parse().expect("Failed to parse binance stream endpoint")
    }

    fn default_subscription() -> Vec<String> {
        vec![
            "btcusdt@aggTrade".to_string(),
            "btcusdt@avgPrice".to_string(),
            "btcusdt@trade".to_string(),
            "btcusdt@kline_1m".to_string(),
            "btcusdt@miniTicker".to_string(),
            "btcusdt@bookTicker".to_string(),
            "btcusdt@depth".to_string(), // interval: 100ms / 1000ms
            "btcusdt@depth5@1000ms".to_string(), // level: 5 / 10 / 20, interval: 100ms / 1000ms
            "btcusdt@ticker".to_string(),
            "btcusdt@ticker_1h".to_string(), // window_size: 1h / 4h / 1d
        ]
    }

    pub fn subscription_endpoint(&self) -> Result<http::Uri, ConfigError::Error> {
        let uri = format!("{}stream?streams={}", self.stream_endpoint, self.subscription.join("/"));

        uri.parse().context(ConfigError::ParseBinanceUriSnafu { uri })
    }
}
