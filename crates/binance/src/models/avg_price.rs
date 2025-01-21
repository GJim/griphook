use crate::{
    database::{ClickhouseDB, Database, PostgresDB, Storage},
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
    "name": "AvgPrice",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "i", "type": "string"},
        {"name": "w", "type": "string"},
        {"name": "T", "type": "long"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AvgPrice {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "w")]
    pub average_price: String,
    #[serde(rename = "T")]
    pub last_trade_time: i64,
}

impl Avro for AvgPrice {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct AvgPriceRow {
    pub interval: String,
    pub average_price: f64,
    pub last_trade_time: i64,
}

impl TryFrom<AvgPrice> for AvgPriceRow {
    type Error = error::Error;

    fn try_from(price: AvgPrice) -> Result<Self> {
        Ok(Self {
            interval: price.interval,
            average_price: price.average_price.parse().context(error::ParseF64Snafu)?,
            last_trade_time: price.last_trade_time,
        })
    }
}

impl ClickhouseRow for AvgPriceRow {
    type Row = Self;
}

impl Storage<clickhouse::Client> for AvgPriceRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                interval String,
                average_price Float64,
                last_trade_time Int64
            ) ENGINE = MergeTree()
            ORDER BY (interval, last_trade_time)
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

impl Storage<Pool<Postgres>> for AvgPriceRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                interval VARCHAR(30) NOT NULL,
                average_price DOUBLE PRECISION NOT NULL,
                last_trade_time BIGINT NOT NULL,
                PRIMARY KEY (interval, last_trade_time)
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
            "INSERT INTO {table_name} (interval, average_price, last_trade_time) "
        ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.interval)
                    .push_bind(row.average_price)
                    .push_bind(row.last_trade_time);
            })
            .push(" ON CONFLICT (interval, last_trade_time) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}

impl Database<AvgPrice, AvgPriceRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                interval String,
                average_price Float64,
                last_trade_time Int64
            ) ENGINE = MergeTree()
            ORDER BY (interval, last_trade_time)
            "#,
        );
        self.client.query(&query).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: AvgPrice) -> Result<AvgPriceRow> {
        AvgPriceRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: AvgPriceRow) -> Result<()> {
        AvgPriceRow::insert_row(&self.client, table_name, data).await
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<AvgPriceRow>) -> Result<()> {
        AvgPriceRow::insert_row_batch(&self.client, table_name, data).await
    }
}

impl Database<AvgPrice, AvgPriceRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                interval VARCHAR(30) NOT NULL,
                average_price DOUBLE PRECISION NOT NULL,
                last_trade_time BIGINT NOT NULL,
                PRIMARY KEY (interval, last_trade_time)
            )
            "#,
        );
        let _unused =
            sqlx::query(&query).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: AvgPrice) -> Result<AvgPriceRow> {
        AvgPriceRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: AvgPriceRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                interval, average_price, last_trade_time
            ) VALUES ($1, $2, $3)
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.interval)
            .bind(data.average_price)
            .bind(data.last_trade_time)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<AvgPriceRow>) -> Result<()> {
        let mut query_builder = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {table_name} (interval, average_price, last_trade_time) "
        ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.interval)
                    .push_bind(row.average_price)
                    .push_bind(row.last_trade_time);
            })
            .push(" ON CONFLICT (interval, last_trade_time) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
