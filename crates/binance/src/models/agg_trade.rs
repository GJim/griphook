use crate::{
    database::Storage,
    models::{
        error::{self, Result},
        Avro, ClickhouseRow,
    },
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sqlx::{Pool, Postgres, QueryBuilder};

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "AggTrade",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "a", "type": "long"},
        {"name": "p", "type": "string"},
        {"name": "q", "type": "string"},
        {"name": "f", "type": "long"},
        {"name": "l", "type": "long"},
        {"name": "T", "type": "long"},
        {"name": "m", "type": "boolean"},
        {"name": "M", "type": "boolean"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub aggregate_trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "l")]
    pub last_trade_id: i64,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_market_maker: bool,
    #[serde(rename = "M", default = "default_ignore")]
    pub ignore: bool,
}

const fn default_ignore() -> bool {
    false
}

impl Avro for AggTrade {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct AggTradeRow {
    pub aggregate_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub trade_time: i64,
    pub is_buyer_market_maker: bool,
}

impl TryFrom<AggTrade> for AggTradeRow {
    type Error = error::Error;

    fn try_from(trade: AggTrade) -> Result<Self> {
        Ok(Self {
            aggregate_trade_id: trade.aggregate_trade_id,
            price: trade.price.parse().context(error::ParseF64Snafu)?,
            quantity: trade.quantity.parse().context(error::ParseF64Snafu)?,
            first_trade_id: trade.first_trade_id,
            last_trade_id: trade.last_trade_id,
            trade_time: trade.trade_time,
            is_buyer_market_maker: trade.is_buyer_market_maker,
        })
    }
}

impl ClickhouseRow for AggTradeRow {
    type Row = Self;
}

impl Storage<clickhouse::Client> for AggTradeRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                aggregate_trade_id Int64,
                price Float64,
                quantity Float64,
                first_trade_id Int64,
                last_trade_id Int64,
                trade_time Int64,
                is_buyer_market_maker Boolean
            ) ENGINE = MergeTree()
            ORDER BY (aggregate_trade_id)
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

impl Storage<Pool<Postgres>> for AggTradeRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                aggregate_trade_id BIGINT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                quantity DOUBLE PRECISION NOT NULL,
                first_trade_id BIGINT NOT NULL,
                last_trade_id BIGINT NOT NULL,
                trade_time BIGINT NOT NULL,
                is_buyer_market_maker BOOLEAN NOT NULL,
                PRIMARY KEY (aggregate_trade_id)
            )
            "#,
        );
        let _unused = sqlx::query(&query).execute(client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn batch_insert(
        client: &Pool<Postgres>,
        table_name: &str,
        data: Vec<Self>,
    ) -> Result<()> {
        let mut query_builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {table_name} (aggregate_trade_id, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_market_maker) "
        ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.aggregate_trade_id)
                    .push_bind(row.price)
                    .push_bind(row.quantity)
                    .push_bind(row.first_trade_id)
                    .push_bind(row.last_trade_id)
                    .push_bind(row.trade_time)
                    .push_bind(row.is_buyer_market_maker);
            })
            .push(" ON CONFLICT (aggregate_trade_id) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
