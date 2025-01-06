use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Ticker",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "p", "type": "string"},
        {"name": "P", "type": "string"},
        {"name": "w", "type": "string"},
        {"name": "x", "type": "string"},
        {"name": "c", "type": "string"},
        {"name": "Q", "type": "string"},
        {"name": "b", "type": "string"},
        {"name": "B", "type": "string"},
        {"name": "a", "type": "string"},
        {"name": "A", "type": "string"},
        {"name": "o", "type": "string"},
        {"name": "h", "type": "string"},
        {"name": "l", "type": "string"},
        {"name": "v", "type": "string"},
        {"name": "q", "type": "string"},
        {"name": "O", "type": "long"},
        {"name": "C", "type": "long"},
        {"name": "F", "type": "long"},
        {"name": "L", "type": "long"},
        {"name": "n", "type": "long"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Ticker {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price_change: String,
    #[serde(rename = "P")]
    pub price_change_percent: String,
    #[serde(rename = "w")]
    pub weighted_avg_price: String,
    #[serde(rename = "x")]
    pub first_trade_price: String,
    #[serde(rename = "c")]
    pub last_price: String,
    #[serde(rename = "Q")]
    pub last_quantity: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "B")]
    pub best_bid_quantity: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
    #[serde(rename = "A")]
    pub best_ask_quantity: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub total_traded_base_asset_volume: String,
    #[serde(rename = "q")]
    pub total_traded_quote_asset_volume: String,
    #[serde(rename = "O")]
    pub statistics_open_time: i64,
    #[serde(rename = "C")]
    pub statistics_close_time: i64,
    #[serde(rename = "F")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "n")]
    pub total_trades: i64,
}

impl Avro for Ticker {
    fn raw_schema() -> &'static str { RAW_SCHEMA }
}
