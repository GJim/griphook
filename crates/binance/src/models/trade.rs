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

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct TradeRow {
    pub trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub trade_time: i64,
    pub is_buyer_market_maker: bool,
}

impl ClickhouseRow for TradeRow {
    type Row = Self;
}

impl TryFrom<Trade> for TradeRow {
    type Error = error::Error;
    fn try_from(value: Trade) -> Result<Self> {
        Ok(Self {
            trade_id: value.trade_id,
            price: value.price.parse().context(error::ParseF64Snafu)?,
            quantity: value.quantity.parse().context(error::ParseF64Snafu)?,
            trade_time: value.trade_time,
            is_buyer_market_maker: value.is_buyer_market_maker,
        })
    }
}

impl Storage<clickhouse::Client> for TradeRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                trade_id Int64,
                price Float64,
                quantity Float64,
                trade_time Int64,
                is_buyer_market_maker Bool
            ) ENGINE = MergeTree()
            ORDER BY (trade_id)
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

impl Storage<Pool<Postgres>> for TradeRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    trade_id BIGINT,
                    price DOUBLE PRECISION,
                    quantity DOUBLE PRECISION,
                    trade_time BIGINT,
                    is_buyer_market_maker BOOLEAN,
                    PRIMARY KEY (trade_id)
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

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            format!(
                "INSERT INTO {table_name} (trade_id, price, quantity, trade_time, is_buyer_market_maker) "
            ),
        );

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.trade_id)
                    .push_bind(row.price)
                    .push_bind(row.quantity)
                    .push_bind(row.trade_time)
                    .push_bind(row.is_buyer_market_maker);
            })
            .push(" ON CONFLICT (trade_id) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
