use crate::{
    models::{
        error::{self, Result},
        Avro, Order, OrderRow,
    },
    ClickhouseDB, Database, PostgresDB,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

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

#[derive(Row, Serialize, sqlx::FromRow)]
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

impl Database<BookDepth, BookDepthRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time Int64,
                    first_update_id Int64,
                    final_update_id Int64,
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
                ORDER BY (event_time)
                "#
        );
        self.client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: BookDepth) -> Result<BookDepthRow> {
        BookDepthRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: BookDepthRow) -> Result<()> {
        let mut insert =
            self.client.insert::<BookDepthRow>(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}

impl Database<BookDepth, BookDepthRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
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
            sqlx::query(&create_table).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: BookDepth) -> Result<BookDepthRow> {
        BookDepthRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: BookDepthRow) -> Result<()> {
        let _unused = sqlx::query(&format!(
            r#"
                INSERT INTO {table_name} (
                    event_time, first_update_id, final_update_id,
                    bids, asks
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (event_time) DO NOTHING
            "#
        ))
        .bind(data.event_time)
        .bind(data.first_update_id)
        .bind(data.final_update_id)
        .bind(&serde_json::to_value(&data.bids).context(error::ParseJsonSnafu)?)
        .bind(&serde_json::to_value(&data.asks).context(error::ParseJsonSnafu)?)
        .execute(&self.client)
        .await
        .context(error::PostgresSnafu)?;
        Ok(())
    }
}
