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
    "name": "BookTicker",
    "fields": [
        {"name": "u", "type": "long"},
        {"name": "s", "type": "string"},
        {"name": "b", "type": "string"},
        {"name": "B", "type": "string"},
        {"name": "a", "type": "string"},
        {"name": "A", "type": "string"}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BookTicker {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "B")]
    pub best_bid_quantity: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
    #[serde(rename = "A")]
    pub best_ask_quantity: String,
}

impl Avro for BookTicker {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct BookTickerRow {
    pub update_id: i64,
    pub best_bid_price: f64,
    pub best_bid_quantity: f64,
    pub best_ask_price: f64,
    pub best_ask_quantity: f64,
}

impl TryFrom<BookTicker> for BookTickerRow {
    type Error = error::Error;

    fn try_from(ticker: BookTicker) -> Result<Self> {
        Ok(Self {
            update_id: ticker.update_id,
            best_bid_price: ticker.best_bid_price.parse().context(error::ParseF64Snafu)?,
            best_bid_quantity: ticker.best_bid_quantity.parse().context(error::ParseF64Snafu)?,
            best_ask_price: ticker.best_ask_price.parse().context(error::ParseF64Snafu)?,
            best_ask_quantity: ticker.best_ask_quantity.parse().context(error::ParseF64Snafu)?,
        })
    }
}

impl Database<BookTicker, BookTickerRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                update_id Int64,
                best_bid_price Float64,
                best_bid_quantity Float64,
                best_ask_price Float64,
                best_ask_quantity Float64
            ) ENGINE = MergeTree()
            ORDER BY (update_id)
            "#,
        );
        self.client.query(&query).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: BookTicker) -> Result<BookTickerRow> {
        BookTickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: BookTickerRow) -> Result<()> {
        let mut insert = self.client.insert(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<BookTickerRow>) -> Result<()> {
        let mut insert = self.client.insert(table_name).context(error::ClickhouseSnafu)?;
        for row in data {
            insert.write(&row).await.context(error::ClickhouseSnafu)?;
        }
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}

impl Database<BookTicker, BookTickerRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                update_id BIGINT NOT NULL,
                best_bid_price DOUBLE PRECISION NOT NULL,
                best_bid_quantity DOUBLE PRECISION NOT NULL,
                best_ask_price DOUBLE PRECISION NOT NULL,
                best_ask_quantity DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (update_id)
            )
            "#,
        );
        let _unused =
            sqlx::query(&query).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: BookTicker) -> Result<BookTickerRow> {
        BookTickerRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: BookTickerRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                update_id, best_bid_price, best_bid_quantity,
                best_ask_price, best_ask_quantity
            ) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (update_id) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.update_id)
            .bind(data.best_bid_price)
            .bind(data.best_bid_quantity)
            .bind(data.best_ask_price)
            .bind(data.best_ask_quantity)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<BookTickerRow>) -> Result<()> {
        let mut query_builder: sqlx::QueryBuilder<'_, sqlx::Postgres> = sqlx::QueryBuilder::new(
            format!("INSERT INTO {table_name} (update_id, best_bid_price, best_bid_quantity, best_ask_price, best_ask_quantity) "),
        );

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.update_id)
                    .push_bind(row.best_bid_price)
                    .push_bind(row.best_bid_quantity)
                    .push_bind(row.best_ask_price)
                    .push_bind(row.best_ask_quantity);
            })
            .push(" ON CONFLICT (update_id) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
