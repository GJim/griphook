use serde::{Deserialize, Serialize};

use super::Avro;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "MarkPrice",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "p", "type": "string"},
        {"name": "i", "type": "string"},
        {"name": "P", "type": "string"},
        {"name": "r", "type": "string"},
        {"name": "T", "type": "long"}
    ]
}
"#;

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarkPrice {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
    #[serde(rename = "i")]
    pub index_price: String,
    #[serde(rename = "P")]
    pub estimated_settle_price: String,
    #[serde(rename = "r")]
    pub funding_rate: String,
    #[serde(rename = "T")]
    pub next_funding_time: i64,
}

impl Avro for MarkPrice {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}
