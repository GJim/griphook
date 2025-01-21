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
    "name": "BookDepth",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "U", "type": "long"},
        {"name": "u", "type": "long"},
        {
            "name": "b",
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
            "name": "a",
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
pub struct BookDepth {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub final_update_id: i64,
    #[serde(rename = "b")]
    pub bids: Vec<Order>,
    #[serde(rename = "a")]
    pub asks: Vec<Order>,
}

impl Avro for BookDepth {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Serialize, sqlx::FromRow)]
pub struct BookDepthRow {
    pub event_time: i64,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub bids: Vec<OrderRow>,
    pub asks: Vec<OrderRow>,
}

impl TryFrom<BookDepth> for BookDepthRow {
    type Error = error::Error;

    fn try_from(data: BookDepth) -> Result<Self> {
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

        Ok(Self {
            event_time: data.event_time,
            first_update_id: data.first_update_id,
            final_update_id: data.final_update_id,
            bids,
            asks,
        })
    }
}

#[derive(Row, Serialize, Debug)]
pub struct BookDepthNestedRow {
    pub event_time: i64,
    pub first_update_id: i64,
    pub final_update_id: i64,
    #[serde(rename = "bids.price")]
    pub bids_price: Vec<f64>,
    #[serde(rename = "bids.quantity")]
    pub bids_quantity: Vec<f64>,
    #[serde(rename = "asks.price")]
    pub asks_price: Vec<f64>,
    #[serde(rename = "asks.quantity")]
    pub asks_quantity: Vec<f64>,
}

impl TryFrom<BookDepth> for BookDepthNestedRow {
    type Error = error::Error;

    fn try_from(data: BookDepth) -> Result<Self> {
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
            event_time: data.event_time,
            first_update_id: data.first_update_id,
            final_update_id: data.final_update_id,
            bids_price: bid_prices,
            bids_quantity: bid_quantities,
            asks_price: ask_prices,
            asks_quantity: ask_quantities,
        })
    }
}

impl ClickhouseRow for BookDepthNestedRow {
    type Row = Self;
}

impl Storage<clickhouse::Client> for BookDepthNestedRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time Int64,
                first_update_id Int64,
                final_update_id Int64,
                bid_prices Array(Float64),
                bid_quantities Array(Float64),
                ask_prices Array(Float64),
                ask_quantities Array(Float64)
            ) ENGINE = MergeTree()
            ORDER BY (event_time)
            "#,
        );
        client.query(&query).execute().await.context(error::ClickhouseSnafu)
    }

    async fn batch_insert(
        client: &clickhouse::Client,
        table_name: &str,
        data: Vec<Self>,
    ) -> Result<()> {
        Self::insert_row_batch(client, table_name, data).await
    }
}

impl Storage<Pool<Postgres>> for BookDepthRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time BIGINT,
                    first_update_id BIGINT,
                    final_update_id BIGINT,
                    bids JSONB,
                    asks JSONB,
                    PRIMARY KEY (event_time)
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
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {table_name} (event_time, first_update_id, final_update_id, bids, asks) "
        ));

        // Pre-serialize the JSON values to handle potential errors
        let processed_data: Result<Vec<_>> = data
            .into_iter()
            .map(|row| {
                let bids_json = serde_json::to_value(&row.bids).context(error::ParseJsonSnafu)?;
                let asks_json = serde_json::to_value(&row.asks).context(error::ParseJsonSnafu)?;
                Ok((row.event_time, row.first_update_id, row.final_update_id, bids_json, asks_json))
            })
            .collect();
        let processed_data = processed_data?;

        let _unused = query_builder
            .push_values(
                processed_data,
                |mut b, (event_time, first_update_id, final_update_id, bids, asks)| {
                    let _unused = b
                        .push_bind(event_time)
                        .push_bind(first_update_id)
                        .push_bind(final_update_id)
                        .push_bind(bids)
                        .push_bind(asks);
                },
            )
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
