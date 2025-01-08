use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "BookTicker",
    "fields": [
        {"name": "u", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "b", "type": "string"},
        {"name": "B", "type": "string"},
        {"name": "a", "type": "string"},
        {"name": "A", "type": "string"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BookTicker {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "B")]
    pub best_bid_quantity: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
    #[serde(rename = "A")]
    pub best_ask_quantity: String,
}

impl Avro for BookTicker {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}
