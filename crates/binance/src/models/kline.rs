use crate::{
    models::{
        error::{self, Result},
        Avro,
    },
    ClickhouseDB, Database, PostgresDB,
};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub const RAW_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "Kline",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "k", "type": {
            "type": "record",
            "name": "KlineData",
            "fields": [
                {"name": "t", "type": "long"},
                {"name": "T", "type": "long"},
                {"name": "s", "type": "string"},
                {"name": "i", "type": "string"},
                {"name": "f", "type": "long"},
                {"name": "L", "type": "long"},
                {"name": "o", "type": "string"},
                {"name": "c", "type": "string"},
                {"name": "h", "type": "string"},
                {"name": "l", "type": "string"},
                {"name": "v", "type": "string"},
                {"name": "n", "type": "long"},
                {"name": "x", "type": "boolean"},
                {"name": "q", "type": "string"},
                {"name": "V", "type": "string"},
                {"name": "Q", "type": "string"},
                {"name": "B", "type": "string"}
            ]
        }}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Kline {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: KlineData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KlineData {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub base_asset_volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_base_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

impl Avro for Kline {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct KlineRow {
    pub event_time: i64,
    pub start_time: i64,
    pub close_time: i64,
    pub interval: String,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub open_price: f64,
    pub close_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub base_asset_volume: f64,
    pub number_of_trades: i64,
    pub is_closed: bool,
    pub quote_asset_volume: f64,
    pub taker_buy_base_volume: f64,
    pub taker_buy_quote_volume: f64,
}

impl TryFrom<Kline> for KlineRow {
    type Error = error::Error;

    fn try_from(data: Kline) -> Result<Self> {
        Ok(Self {
            event_time: data.event_time,
            start_time: data.kline.start_time,
            close_time: data.kline.close_time,
            interval: data.kline.interval,
            first_trade_id: data.kline.first_trade_id,
            last_trade_id: data.kline.last_trade_id,
            open_price: data.kline.open_price.parse().context(error::ParseF64Snafu)?,
            close_price: data.kline.close_price.parse().context(error::ParseF64Snafu)?,
            high_price: data.kline.high_price.parse().context(error::ParseF64Snafu)?,
            low_price: data.kline.low_price.parse().context(error::ParseF64Snafu)?,
            base_asset_volume: data
                .kline
                .base_asset_volume
                .parse()
                .context(error::ParseF64Snafu)?,
            number_of_trades: data.kline.number_of_trades,
            is_closed: data.kline.is_closed,
            quote_asset_volume: data
                .kline
                .quote_asset_volume
                .parse()
                .context(error::ParseF64Snafu)?,
            taker_buy_base_volume: data
                .kline
                .taker_buy_base_volume
                .parse()
                .context(error::ParseF64Snafu)?,
            taker_buy_quote_volume: data
                .kline
                .taker_buy_quote_volume
                .parse()
                .context(error::ParseF64Snafu)?,
        })
    }
}

impl Database<Kline, KlineRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time Int64,
                    start_time Int64,
                    close_time Int64,
                    interval String,
                    first_trade_id Int64,
                    last_trade_id Int64,
                    open_price Float64,
                    close_price Float64,
                    high_price Float64,
                    low_price Float64,
                    base_asset_volume Float64,
                    number_of_trades Int64,
                    is_closed Bool,
                    quote_asset_volume Float64,
                    taker_buy_base_volume Float64,
                    taker_buy_quote_volume Float64
                )
                ENGINE = MergeTree()
                ORDER BY (event_time)
                "#
        );
        self.client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Kline) -> Result<KlineRow> {
        KlineRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: KlineRow) -> Result<()> {
        let mut insert =
            self.client.insert::<KlineRow>(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<KlineRow>) -> Result<()> {
        let mut insert =
            self.client.insert::<KlineRow>(table_name).context(error::ClickhouseSnafu)?;
        for row in data {
            insert.write(&row).await.context(error::ClickhouseSnafu)?;
        }
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}

impl Database<Kline, KlineRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    event_time BIGINT,
                    start_time BIGINT,
                    close_time BIGINT,
                    interval VARCHAR,
                    first_trade_id BIGINT,
                    last_trade_id BIGINT,
                    open_price DOUBLE PRECISION,
                    close_price DOUBLE PRECISION,
                    high_price DOUBLE PRECISION,
                    low_price DOUBLE PRECISION,
                    base_asset_volume DOUBLE PRECISION,
                    number_of_trades BIGINT,
                    is_closed BOOLEAN,
                    quote_asset_volume DOUBLE PRECISION,
                    taker_buy_base_volume DOUBLE PRECISION,
                    taker_buy_quote_volume DOUBLE PRECISION,
                    PRIMARY KEY (event_time)
                )
                "#
        );
        let _unused =
            sqlx::query(&create_table).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Kline) -> Result<KlineRow> {
        KlineRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: KlineRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                event_time, start_time, close_time, interval,
                first_trade_id, last_trade_id, open_price, close_price,
                high_price, low_price, base_asset_volume, number_of_trades,
                is_closed, quote_asset_volume, taker_buy_base_volume,
                taker_buy_quote_volume
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16
            ) ON CONFLICT (event_time) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.event_time)
            .bind(data.start_time)
            .bind(data.close_time)
            .bind(data.interval)
            .bind(data.first_trade_id)
            .bind(data.last_trade_id)
            .bind(data.open_price)
            .bind(data.close_price)
            .bind(data.high_price)
            .bind(data.low_price)
            .bind(data.base_asset_volume)
            .bind(data.number_of_trades)
            .bind(data.is_closed)
            .bind(data.quote_asset_volume)
            .bind(data.taker_buy_base_volume)
            .bind(data.taker_buy_quote_volume)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<KlineRow>) -> Result<()> {
        let mut query_builder: sqlx::QueryBuilder<'_, sqlx::Postgres> =
            sqlx::QueryBuilder::new(format!(
                "INSERT INTO {table_name} (event_time, start_time, close_time, interval, 
                first_trade_id, last_trade_id, open_price, close_price, high_price, low_price, 
                base_asset_volume, number_of_trades, is_closed, quote_asset_volume, 
                taker_buy_base_volume, taker_buy_quote_volume) "
            ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.event_time)
                    .push_bind(row.start_time)
                    .push_bind(row.close_time)
                    .push_bind(row.interval)
                    .push_bind(row.first_trade_id)
                    .push_bind(row.last_trade_id)
                    .push_bind(row.open_price)
                    .push_bind(row.close_price)
                    .push_bind(row.high_price)
                    .push_bind(row.low_price)
                    .push_bind(row.base_asset_volume)
                    .push_bind(row.number_of_trades)
                    .push_bind(row.is_closed)
                    .push_bind(row.quote_asset_volume)
                    .push_bind(row.taker_buy_base_volume)
                    .push_bind(row.taker_buy_quote_volume);
            })
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
