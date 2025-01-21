use crate::{
    database::Storage,
    models::{
        error::{self, Result},
        Avro, ClickhouseRow, Order, OrderRow,
    },
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sqlx::{Pool, Postgres, QueryBuilder};

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
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct PartialBookDepthRow {
    pub last_update_id: i64,
    pub bids: Vec<OrderRow>,
    pub asks: Vec<OrderRow>,
}

impl TryFrom<PartialBookDepth> for PartialBookDepthRow {
    type Error = error::Error;

    fn try_from(data: PartialBookDepth) -> Result<Self> {
        let mut bids = Vec::with_capacity(data.bids.len());
        let mut asks = Vec::with_capacity(data.asks.len());

        for bid in data.bids {
            let order_row = OrderRow::try_from(bid)?;
            bids.push(order_row);
        }

        for ask in data.asks {
            let order_row = OrderRow::try_from(ask)?;
            asks.push(order_row);
        }

        Ok(Self { last_update_id: data.last_update_id, bids, asks })
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct PartialBookDepthNestedRow {
    pub last_update_id: i64,
    #[serde(rename = "bids.price")]
    pub bids_price: Vec<f64>,
    #[serde(rename = "bids.quantity")]
    pub bids_quantity: Vec<f64>,
    #[serde(rename = "asks.price")]
    pub asks_price: Vec<f64>,
    #[serde(rename = "asks.quantity")]
    pub asks_quantity: Vec<f64>,
}

impl ClickhouseRow for PartialBookDepthNestedRow {
    type Row = Self;
}

impl TryFrom<PartialBookDepth> for PartialBookDepthNestedRow {
    type Error = error::Error;

    fn try_from(data: PartialBookDepth) -> Result<Self> {
        let mut bid_prices = Vec::with_capacity(data.bids.len());
        let mut bid_quantities = Vec::with_capacity(data.bids.len());

        for bid in data.bids {
            bid_prices.push(bid.price.parse().context(error::ParseF64Snafu)?);
            bid_quantities.push(bid.quantity.parse().context(error::ParseF64Snafu)?);
        }

        let mut ask_prices = Vec::with_capacity(data.asks.len());
        let mut ask_quantities = Vec::with_capacity(data.asks.len());

        for ask in data.asks {
            ask_prices.push(ask.price.parse().context(error::ParseF64Snafu)?);
            ask_quantities.push(ask.quantity.parse().context(error::ParseF64Snafu)?);
        }

        Ok(Self {
            last_update_id: data.last_update_id,
            bids_price: bid_prices,
            bids_quantity: bid_quantities,
            asks_price: ask_prices,
            asks_quantity: ask_quantities,
        })
    }
}

impl Storage<clickhouse::Client> for PartialBookDepthNestedRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                last_update_id Int64,
                bids Nested (
                    price Float64,
                    quantity Float64
                ),
                asks Nested (
                    price Float64,
                    quantity Float64
                )
            )
            ENGINE = MergeTree()
            ORDER BY (last_update_id)
            "#
        );
        client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn batch_insert(
        client: &clickhouse::Client,
        table_name: &str,
        data: Vec<Self>,
    ) -> Result<()> {
        Self::insert_row_batch(client, table_name, data).await
    }
}

impl Storage<Pool<Postgres>> for PartialBookDepthRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                last_update_id BIGINT NOT NULL,
                bids JSONB NOT NULL,
                asks JSONB NOT NULL,
                PRIMARY KEY (last_update_id)
            )
            "#
        );
        let _unused =
            sqlx::query(&create_table).execute(client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn batch_insert(
        client: &Pool<Postgres>,
        table_name: &str,
        data: Vec<Self>,
    ) -> Result<()> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new(format!("INSERT INTO {table_name} (last_update_id, bids, asks) "));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.last_update_id)
                    .push_bind(serde_json::to_value(&row.bids).unwrap())
                    .push_bind(serde_json::to_value(&row.asks).unwrap());
            })
            .push(" ON CONFLICT (last_update_id) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
