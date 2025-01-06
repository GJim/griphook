use serde::{Deserialize, Serialize};

use super::{Avro, Order};

// pub const RAW_SCHEMA: &str = r#"
// {
//     "type": "record",
//     "name": "PartialBookDepth",
//     "fields": [
//         {"name": "lastUpdateId", "type": "long"},
//         {
//             "name": "bids",
//             "type": {
//                 "type": "array",
//                 "items": {
//                     "type": "array",
//                     "items": "string"
//                 }
//             }
//         },
//         {
//             "name": "asks",
//             "type": {
//                 "type": "array",
//                 "items": {
//                     "type": "array",
//                     "items": "string"
//                 }
//             }
//         }
//     ]
// }
// "#;
pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "PartialBookDepth",
    "fields": [
        {"name": "lastUpdateId", "type": "long"},
        {
            "name": "bids",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Order",
                    "fields": [
                        {"name": "p", "type": "string"},
                        {"name": "q", "type": "string"}
                    ]
                }
            }
        },
        {
            "name": "asks",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Order",
                    "fields": [
                        {"name": "p", "type": "string"},
                        {"name": "q", "type": "string"}
                    ]
                }
            }
        }
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartialBookDepth {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
}

impl Avro for PartialBookDepth {
    fn raw_schema() -> &'static str { RAW_SCHEMA }
}
