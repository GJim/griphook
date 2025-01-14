use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::ConfigError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BinanceConfig {
    #[serde(default = "BinanceConfig::default_trading_type")]
    pub trading_type: TradingType,

    #[serde(default = "BinanceConfig::spot_subscription")]
    pub subscription: Vec<String>,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self { trading_type: Self::default_trading_type(), subscription: Self::spot_subscription() }
    }
}

#[allow(dead_code)]
impl BinanceConfig {
    const fn default_trading_type() -> TradingType {
        TradingType::Spot
    }

    fn spot_subscription() -> Vec<String> {
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

    fn future_subscription() -> Vec<String> {
        vec![
            "btcusdt@aggTrade".to_string(),
            "btcusdt@markPrice".to_string(),
            "btcusdt_perpetual@continuousKline_1m".to_string(), // window_size: 1m / 3m / 5m / 15m / 30m / 1h / 2h / 4h / 6h / 8h / 12h / 1d / 3d / 1w / 1M
            "btcusdt@forceOrder".to_string(),
        ]
    }

    pub fn subscription_endpoint(&self) -> Result<http::Uri, ConfigError::Error> {
        let uri = format!(
            "{}stream?streams={}",
            self.trading_type.stream_endpoint(),
            self.subscription.join("/")
        );

        uri.parse().context(ConfigError::ParseBinanceUriSnafu { uri })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TradingType {
    TestSpot,
    Spot,
    USDFutures,
    COINFutures,
}

impl std::fmt::Display for TradingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TestSpot => write!(f, "test_spot"),
            Self::Spot => write!(f, "spot"),
            Self::USDFutures => write!(f, "usd_futures"),
            Self::COINFutures => write!(f, "coin_futures"),
        }
    }
}

impl TradingType {
    pub fn stream_endpoint(&self) -> http::Uri {
        match self {
            Self::TestSpot => "wss://testnet.binance.vision"
                .parse()
                .expect("Failed to parse binance test spot stream endpoint"),
            Self::Spot => "wss://stream.binance.com:9443"
                .parse()
                .expect("Failed to parse binance spot stream endpoint"),
            Self::USDFutures => "wss://fstream.binance.com"
                .parse()
                .expect("Failed to parse binance usd futures stream endpoint"),
            Self::COINFutures => "wss://dstream.binance.com"
                .parse()
                .expect("Failed to parse binance coin futures stream endpoint"),
        }
    }
}
