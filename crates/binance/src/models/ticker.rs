use crate::{
    models::{
        error::{self, Result},
        Avro, ClickhouseRow,
    },
    ClickhouseDB, Database, PostgresDB,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Ticker",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "p", "type": "string"},
        {"name": "P", "type": "string"},
        {"name": "w", "type": "string"},
        {"name": "x", "type": "string"},
        {"name": "c", "type": "string"},
        {"name": "Q", "type": "string"},
        {"name": "b", "type": "string"},
        {"name": "B", "type": "string"},
        {"name": "a", "type": "string"},
        {"name": "A", "type": "string"},
        {"name": "o", "type": "string"},
        {"name": "h", "type": "string"},
        {"name": "l", "type": "string"},
        {"name": "v", "type": "string"},
        {"name": "q", "type": "string"},
        {"name": "O", "type": "long"},
        {"name": "C", "type": "long"},
        {"name": "F", "type": "long"},
        {"name": "L", "type": "long"},
        {"name": "n", "type": "long"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Ticker {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price_change: String,
    #[serde(rename = "P")]
    pub price_change_percent: String,
    #[serde(rename = "w")]
    pub weighted_avg_price: String,
    #[serde(rename = "x")]
    pub first_trade_price: String,
    #[serde(rename = "c")]
    pub last_price: String,
    #[serde(rename = "Q")]
    pub last_quantity: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "B")]
    pub best_bid_quantity: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
    #[serde(rename = "A")]
    pub best_ask_quantity: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub total_traded_base_asset_volume: String,
    #[serde(rename = "q")]
    pub total_traded_quote_asset_volume: String,
    #[serde(rename = "O")]
    pub statistics_open_time: i64,
    #[serde(rename = "C")]
    pub statistics_close_time: i64,
    #[serde(rename = "F")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "n")]
    pub total_trades: i64,
}

impl Avro for Ticker {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct TickerRow {
    pub event_time: i64,
    pub price_change: f64,
    pub price_change_percent: f64,
    pub weighted_avg_price: f64,
    pub first_trade_price: f64,
    pub last_price: f64,
    pub last_quantity: f64,
    pub best_bid_price: f64,
    pub best_bid_quantity: f64,
    pub best_ask_price: f64,
    pub best_ask_quantity: f64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub total_traded_base_asset_volume: f64,
    pub total_traded_quote_asset_volume: f64,
    pub statistics_open_time: i64,
    pub statistics_close_time: i64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub total_trades: i64,
}

impl ClickhouseRow for TickerRow {
    type Row = Self;
}

impl TryFrom<Ticker> for TickerRow {
    type Error = error::Error;

    fn try_from(data: Ticker) -> Result<Self> {
        Ok(Self {
            event_time: data.event_time,
            price_change: data.price_change.parse().context(error::ParseF64Snafu)?,
            price_change_percent: data
                .price_change_percent
                .parse()
                .context(error::ParseF64Snafu)?,
            weighted_avg_price: data.weighted_avg_price.parse().context(error::ParseF64Snafu)?,
            first_trade_price: data.first_trade_price.parse().context(error::ParseF64Snafu)?,
            last_price: data.last_price.parse().context(error::ParseF64Snafu)?,
            last_quantity: data.last_quantity.parse().context(error::ParseF64Snafu)?,
            best_bid_price: data.best_bid_price.parse().context(error::ParseF64Snafu)?,
            best_bid_quantity: data.best_bid_quantity.parse().context(error::ParseF64Snafu)?,
            best_ask_price: data.best_ask_price.parse().context(error::ParseF64Snafu)?,
            best_ask_quantity: data.best_ask_quantity.parse().context(error::ParseF64Snafu)?,
            open_price: data.open_price.parse().context(error::ParseF64Snafu)?,
            high_price: data.high_price.parse().context(error::ParseF64Snafu)?,
            low_price: data.low_price.parse().context(error::ParseF64Snafu)?,
            total_traded_base_asset_volume: data
                .total_traded_base_asset_volume
                .parse()
                .context(error::ParseF64Snafu)?,
            total_traded_quote_asset_volume: data
                .total_traded_quote_asset_volume
                .parse()
                .context(error::ParseF64Snafu)?,
            statistics_open_time: data.statistics_open_time,
            statistics_close_time: data.statistics_close_time,
            first_trade_id: data.first_trade_id,
            last_trade_id: data.last_trade_id,
            total_trades: data.total_trades,
        })
    }
}

impl Database<Ticker, TickerRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time Int64,
                    price_change Float64,
                    price_change_percent Float64,
                    weighted_avg_price Float64,
                    first_trade_price Float64,
                    last_price Float64,
                    last_quantity Float64,
                    best_bid_price Float64,
                    best_bid_quantity Float64,
                    best_ask_price Float64,
                    best_ask_quantity Float64,
                    open_price Float64,
                    high_price Float64,
                    low_price Float64,
                    total_traded_base_asset_volume Float64,
                    total_traded_quote_asset_volume Float64,
                    statistics_open_time Int64,
                    statistics_close_time Int64,
                    first_trade_id Int64,
                    last_trade_id Int64,
                    total_trades Int64
                )
                ENGINE = MergeTree()
                ORDER BY (event_time)
                "#
        );
        self.client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Ticker) -> Result<TickerRow> {
        TickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: TickerRow) -> Result<()> {
        TickerRow::insert_row(&self.client, table_name, data).await
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<TickerRow>) -> Result<()> {
        TickerRow::insert_row_batch(&self.client, table_name, data).await
    }
}

impl Database<Ticker, TickerRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time BIGINT,
                    price_change DOUBLE PRECISION,
                    price_change_percent DOUBLE PRECISION,
                    weighted_avg_price DOUBLE PRECISION,
                    first_trade_price DOUBLE PRECISION,
                    last_price DOUBLE PRECISION,
                    last_quantity DOUBLE PRECISION,
                    best_bid_price DOUBLE PRECISION,
                    best_bid_quantity DOUBLE PRECISION,
                    best_ask_price DOUBLE PRECISION,
                    best_ask_quantity DOUBLE PRECISION,
                    open_price DOUBLE PRECISION,
                    high_price DOUBLE PRECISION,
                    low_price DOUBLE PRECISION,
                    total_traded_base_asset_volume DOUBLE PRECISION,
                    total_traded_quote_asset_volume DOUBLE PRECISION,
                    statistics_open_time BIGINT,
                    statistics_close_time BIGINT,
                    first_trade_id BIGINT,
                    last_trade_id BIGINT,
                    total_trades BIGINT,
                    PRIMARY KEY (event_time)
                )
                "#
        );
        let _unused =
            sqlx::query(&create_table).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Ticker) -> Result<TickerRow> {
        TickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: TickerRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                event_time, price_change, price_change_percent, weighted_avg_price,
                first_trade_price, last_price, last_quantity, best_bid_price,
                best_bid_quantity, best_ask_price, best_ask_quantity, open_price,
                high_price, low_price, total_traded_base_asset_volume,
                total_traded_quote_asset_volume, statistics_open_time,
                statistics_close_time, first_trade_id, last_trade_id, total_trades
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20, $21
            )
            ON CONFLICT (event_time) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.event_time)
            .bind(data.price_change)
            .bind(data.price_change_percent)
            .bind(data.weighted_avg_price)
            .bind(data.first_trade_price)
            .bind(data.last_price)
            .bind(data.last_quantity)
            .bind(data.best_bid_price)
            .bind(data.best_bid_quantity)
            .bind(data.best_ask_price)
            .bind(data.best_ask_quantity)
            .bind(data.open_price)
            .bind(data.high_price)
            .bind(data.low_price)
            .bind(data.total_traded_base_asset_volume)
            .bind(data.total_traded_quote_asset_volume)
            .bind(data.statistics_open_time)
            .bind(data.statistics_close_time)
            .bind(data.first_trade_id)
            .bind(data.last_trade_id)
            .bind(data.total_trades)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<TickerRow>) -> Result<()> {
        let mut query_builder: sqlx::QueryBuilder<'_, sqlx::Postgres> = sqlx::QueryBuilder::new(
            format!(
                "INSERT INTO {table_name} (event_time, price_change, price_change_percent, weighted_avg_price, \
                first_trade_price, last_price, last_quantity, best_bid_price, best_bid_quantity, best_ask_price, \
                best_ask_quantity, open_price, high_price, low_price, total_traded_base_asset_volume, \
                total_traded_quote_asset_volume, statistics_open_time, statistics_close_time, first_trade_id, \
                last_trade_id, total_trades) "
            ),
        );

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.event_time)
                    .push_bind(row.price_change)
                    .push_bind(row.price_change_percent)
                    .push_bind(row.weighted_avg_price)
                    .push_bind(row.first_trade_price)
                    .push_bind(row.last_price)
                    .push_bind(row.last_quantity)
                    .push_bind(row.best_bid_price)
                    .push_bind(row.best_bid_quantity)
                    .push_bind(row.best_ask_price)
                    .push_bind(row.best_ask_quantity)
                    .push_bind(row.open_price)
                    .push_bind(row.high_price)
                    .push_bind(row.low_price)
                    .push_bind(row.total_traded_base_asset_volume)
                    .push_bind(row.total_traded_quote_asset_volume)
                    .push_bind(row.statistics_open_time)
                    .push_bind(row.statistics_close_time)
                    .push_bind(row.first_trade_id)
                    .push_bind(row.last_trade_id)
                    .push_bind(row.total_trades);
            })
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
