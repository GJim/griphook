use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "ContinuousKline",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "ps", "type": "string"},
        {"name": "ct", "type": "string"},
        {"name": "k", "type": {
            "type": "record",
            "name": "ContinuousKlineData",
            "fields": [
                {"name": "t", "type": "long"},
                {"name": "T", "type": "long"},
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
pub struct ContinuousKline {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "ps")]
    pub pair: String,
    #[serde(rename = "ct")]
    pub contract_type: String,
    #[serde(rename = "k")]
    pub kline: ContinuousKlineData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContinuousKlineData {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_update_id: i64,
    #[serde(rename = "L")]
    pub last_update_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

impl Avro for ContinuousKline {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}
