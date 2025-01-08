use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Trade",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "t", "type": "long"},
        {"name": "p", "type": "string"},
        {"name": "q", "type": "string"},
        {"name": "T", "type": "long"},
        {"name": "m", "type": "boolean"},
        {"name": "M", "type": "boolean"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Trade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_market_maker: bool,
    #[serde(rename = "M")]
    pub ignore: bool,
}

impl Avro for Trade {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}
