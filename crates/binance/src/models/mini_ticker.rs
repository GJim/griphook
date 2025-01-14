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
    "name": "MiniTicker",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "c", "type": "string"},
        {"name": "o", "type": "string"},
        {"name": "h", "type": "string"},
        {"name": "l", "type": "string"},
        {"name": "v", "type": "string"},
        {"name": "q", "type": "string"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MiniTicker {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub base_volume: String,
    #[serde(rename = "q")]
    pub quote_volume: String,
}

impl Avro for MiniTicker {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct MiniTickerRow {
    pub event_time: i64,
    pub close_price: f64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub base_volume: f64,
    pub quote_volume: f64,
}

impl TryFrom<MiniTicker> for MiniTickerRow {
    type Error = error::Error;

    fn try_from(ticker: MiniTicker) -> Result<Self> {
        Ok(Self {
            event_time: ticker.event_time,
            close_price: ticker.close_price.parse().context(error::ParseF64Snafu)?,
            open_price: ticker.open_price.parse().context(error::ParseF64Snafu)?,
            high_price: ticker.high_price.parse().context(error::ParseF64Snafu)?,
            low_price: ticker.low_price.parse().context(error::ParseF64Snafu)?,
            base_volume: ticker.base_volume.parse().context(error::ParseF64Snafu)?,
            quote_volume: ticker.quote_volume.parse().context(error::ParseF64Snafu)?,
        })
    }
}

impl Database<MiniTicker, MiniTickerRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time Int64,
                close_price Float64,
                open_price Float64,
                high_price Float64,
                low_price Float64,
                base_volume Float64,
                quote_volume Float64
            ) ENGINE = MergeTree()
            ORDER BY (event_time)
            "#,
        );
        self.client.query(&query).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: MiniTicker) -> Result<MiniTickerRow> {
        MiniTickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: MiniTickerRow) -> Result<()> {
        let mut insert = self.client.insert(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}

impl Database<MiniTicker, MiniTickerRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time BIGINT NOT NULL,
                close_price DOUBLE PRECISION NOT NULL,
                open_price DOUBLE PRECISION NOT NULL,
                high_price DOUBLE PRECISION NOT NULL,
                low_price DOUBLE PRECISION NOT NULL,
                base_volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (event_time)
            )
            "#,
        );
        let _unused =
            sqlx::query(&query).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: MiniTicker) -> Result<MiniTickerRow> {
        MiniTickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: MiniTickerRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                event_time, close_price, open_price, high_price,
                low_price, base_volume, quote_volume
            ) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (event_time) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.event_time)
            .bind(data.close_price)
            .bind(data.open_price)
            .bind(data.high_price)
            .bind(data.low_price)
            .bind(data.base_volume)
            .bind(data.quote_volume)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }
}
