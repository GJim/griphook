use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "ForceOrder",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "o", "type": {
            "type": "record",
            "name": "ForceOrderData",
            "fields": [
                {"name": "s", "type": "string"},
                {"name": "S", "type": "string"},
                {"name": "o", "type": "string"},
                {"name": "f", "type": "string"},
                {"name": "q", "type": "string"},
                {"name": "p", "type": "string"},
                {"name": "ap", "type": "string"},
                {"name": "X", "type": "string"},
                {"name": "l", "type": "string"},
                {"name": "z", "type": "string"},
                {"name": "T", "type": "long"}
            ]
        }}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrder {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "o")]
    pub order: ForceOrderData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrderData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub original_quantity: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "ap")]
    pub average_price: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "l")]
    pub last_filled_quantity: String,
    #[serde(rename = "z")]
    pub accumulated_filled_quantity: String,
    #[serde(rename = "T")]
    pub trade_time: i64,
}

impl Avro for ForceOrder {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}
