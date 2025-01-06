use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Kline",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "k", "type": {
            "type": "record",
            "name": "KlineData",
            "fields": [
                {"name": "t", "type": "long"},
                {"name": "T", "type": "long"},
                {"name": "s", "type": "string"},
                {"name": "i", "type": "string"},
                {"name": "f", "type": "long"},
                {"name": "L", "type": "long"},
                {"name": "o", "type": "string"},
                {"name": "c", "type": "string"},
                {"name": "h", "type": "string"},
                {"name": "l", "type": "string"},
                {"name": "v", "type": "string"},
                {"name": "n", "type": "long"},
                {"name": "x", "type": "boolean"},
                {"name": "q", "type": "string"},
                {"name": "V", "type": "string"},
                {"name": "Q", "type": "string"},
                {"name": "B", "type": "string"}
            ]
        }}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Kline {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub data: KlineData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KlineData {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub base_asset_volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_base_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

impl Avro for Kline {
    fn raw_schema() -> &'static str { RAW_SCHEMA }
}
