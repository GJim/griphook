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
    "name": "MarkPrice",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "p", "type": "string"},
        {"name": "i", "type": "string"},
        {"name": "P", "type": "string"},
        {"name": "r", "type": "string"},
        {"name": "T", "type": "long"}
    ]
}
"#;

#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarkPrice {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
    #[serde(rename = "i")]
    pub index_price: String,
    #[serde(rename = "P")]
    pub estimated_settle_price: String,
    #[serde(rename = "r")]
    pub funding_rate: String,
    #[serde(rename = "T")]
    pub next_funding_time: i64,
}

impl Avro for MarkPrice {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct MarkPriceRow {
    pub event_time: i64,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_settle_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: i64,
}

impl ClickhouseRow for MarkPriceRow {
    type Row = Self;
}

impl TryFrom<MarkPrice> for MarkPriceRow {
    type Error = error::Error;

    fn try_from(price: MarkPrice) -> Result<Self> {
        Ok(Self {
            event_time: price.event_time,
            mark_price: price.mark_price.parse().context(error::ParseF64Snafu)?,
            index_price: price.index_price.parse().context(error::ParseF64Snafu)?,
            estimated_settle_price: price
                .estimated_settle_price
                .parse()
                .context(error::ParseF64Snafu)?,
            funding_rate: price.funding_rate.parse().context(error::ParseF64Snafu)?,
            next_funding_time: price.next_funding_time,
        })
    }
}

impl Database<MarkPrice, MarkPriceRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time Int64,
                mark_price Float64,
                index_price Float64,
                estimated_settle_price Float64,
                funding_rate Float64,
                next_funding_time Int64
            ) ENGINE = MergeTree()
            ORDER BY (event_time)
            "#,
        );
        self.client.query(&query).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: MarkPrice) -> Result<MarkPriceRow> {
        MarkPriceRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: MarkPriceRow) -> Result<()> {
        MarkPriceRow::insert_row(&self.client, table_name, data).await
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<MarkPriceRow>) -> Result<()> {
        MarkPriceRow::insert_row_batch(&self.client, table_name, data).await
    }
}

impl Database<MarkPrice, MarkPriceRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time BIGINT NOT NULL,
                mark_price DOUBLE PRECISION NOT NULL,
                index_price DOUBLE PRECISION NOT NULL,
                estimated_settle_price DOUBLE PRECISION NOT NULL,
                funding_rate DOUBLE PRECISION NOT NULL,
                next_funding_time BIGINT NOT NULL,
                PRIMARY KEY (event_time)
            )
            "#,
        );
        let _unused =
            sqlx::query(&query).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: MarkPrice) -> Result<MarkPriceRow> {
        MarkPriceRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: MarkPriceRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                event_time, mark_price, index_price, estimated_settle_price,
                funding_rate, next_funding_time
            ) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (event_time) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.event_time)
            .bind(data.mark_price)
            .bind(data.index_price)
            .bind(data.estimated_settle_price)
            .bind(data.funding_rate)
            .bind(data.next_funding_time)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<MarkPriceRow>) -> Result<()> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {table_name} (event_time, mark_price, index_price, estimated_settle_price, 
            funding_rate, next_funding_time) "
        ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.event_time)
                    .push_bind(row.mark_price)
                    .push_bind(row.index_price)
                    .push_bind(row.estimated_settle_price)
                    .push_bind(row.funding_rate)
                    .push_bind(row.next_funding_time);
            })
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}

impl Storage<clickhouse::Client> for MarkPriceRow {
    async fn ensure_table_exists(client: &clickhouse::Client, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time Int64,
                mark_price Float64,
                index_price Float64,
                estimated_settle_price Float64,
                funding_rate Float64,
                next_funding_time Int64
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

impl Storage<Pool<Postgres>> for MarkPriceRow {
    async fn ensure_table_exists(client: &Pool<Postgres>, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time BIGINT NOT NULL,
                mark_price DOUBLE PRECISION NOT NULL,
                index_price DOUBLE PRECISION NOT NULL,
                estimated_settle_price DOUBLE PRECISION NOT NULL,
                funding_rate DOUBLE PRECISION NOT NULL,
                next_funding_time BIGINT NOT NULL,
                PRIMARY KEY (event_time)
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
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {table_name} (event_time, mark_price, index_price, estimated_settle_price, 
            funding_rate, next_funding_time) "
        ));

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.event_time)
                    .push_bind(row.mark_price)
                    .push_bind(row.index_price)
                    .push_bind(row.estimated_settle_price)
                    .push_bind(row.funding_rate)
                    .push_bind(row.next_funding_time);
            })
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
