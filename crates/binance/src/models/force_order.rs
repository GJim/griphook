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
    "name": "ForceOrder",
    "fields": [
        {"name": "e", "type": "string"},
        {"name": "E", "type": "long"},
        {"name": "o", "type": {
            "type": "record",
            "name": "ForceOrderData",
            "fields": [
                {"name": "s", "type": "string"},
                {"name": "S", "type": "string"},
                {"name": "o", "type": "string"},
                {"name": "f", "type": "string"},
                {"name": "q", "type": "string"},
                {"name": "p", "type": "string"},
                {"name": "ap", "type": "string"},
                {"name": "X", "type": "string"},
                {"name": "l", "type": "string"},
                {"name": "z", "type": "string"},
                {"name": "T", "type": "long"}
            ]
        }}
    ]
}
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrder {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "o")]
    pub order: ForceOrderData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrderData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub original_quantity: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "ap")]
    pub average_price: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "l")]
    pub last_filled_quantity: String,
    #[serde(rename = "z")]
    pub accumulated_filled_quantity: String,
    #[serde(rename = "T")]
    pub trade_time: i64,
}

impl Avro for ForceOrder {
    fn raw_schema() -> &'static str {
        RAW_SCHEMA
    }
}

#[derive(Row, Serialize, sqlx::FromRow)]
pub struct ForceOrderRow {
    pub event_time: i64,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub quantity: f64,
    pub price: f64,
    pub average_price: f64,
    pub status: String,
    pub last_filled_quantity: f64,
    pub filled_quantity: f64,
    pub trade_time: i64,
}

impl ClickhouseRow for ForceOrderRow {
    type Row = Self;
}

impl TryFrom<ForceOrder> for ForceOrderRow {
    type Error = error::Error;

    fn try_from(order: ForceOrder) -> Result<Self> {
        Ok(Self {
            event_time: order.event_time,
            side: order.order.side,
            order_type: order.order.order_type,
            time_in_force: order.order.time_in_force,
            quantity: order.order.original_quantity.parse().context(error::ParseF64Snafu)?,
            price: order.order.price.parse().context(error::ParseF64Snafu)?,
            average_price: order.order.average_price.parse().context(error::ParseF64Snafu)?,
            status: order.order.order_status,
            last_filled_quantity: order
                .order
                .last_filled_quantity
                .parse()
                .context(error::ParseF64Snafu)?,
            filled_quantity: order
                .order
                .accumulated_filled_quantity
                .parse()
                .context(error::ParseF64Snafu)?,
            trade_time: order.order.trade_time,
        })
    }
}

impl Database<ForceOrder, ForceOrderRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time Int64,
                side String,
                order_type String,
                time_in_force String,
                quantity Float64,
                price Float64,
                average_price Float64,
                status String,
                last_filled_quantity Float64,
                filled_quantity Float64,
                trade_time Int64
            ) ENGINE = MergeTree()
            ORDER BY (event_time)
            "#,
        );
        self.client.query(&query).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: ForceOrder) -> Result<ForceOrderRow> {
        ForceOrderRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: ForceOrderRow) -> Result<()> {
        ForceOrderRow::insert_row(&self.client, table_name, data).await
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<ForceOrderRow>) -> Result<()> {
        ForceOrderRow::insert_row_batch(&self.client, table_name, data).await
    }
}

impl Database<ForceOrder, ForceOrderRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time BIGINT NOT NULL,
                side VARCHAR(10) NOT NULL,
                order_type VARCHAR(20) NOT NULL,
                time_in_force VARCHAR(10) NOT NULL,
                quantity DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                average_price DOUBLE PRECISION NOT NULL,
                status VARCHAR(20) NOT NULL,
                last_filled_quantity DOUBLE PRECISION NOT NULL,
                filled_quantity DOUBLE PRECISION NOT NULL,
                trade_time BIGINT NOT NULL,
                PRIMARY KEY (event_time)
            )
            "#,
        );
        let _unused =
            sqlx::query(&query).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: ForceOrder) -> Result<ForceOrderRow> {
        ForceOrderRow::try_from(data)
    }

    async fn insert_row(&self, table_name: &str, data: ForceOrderRow) -> Result<()> {
        let query = format!(
            r#"
            INSERT INTO {table_name} (
                event_time, side, order_type, time_in_force,
                quantity, price, average_price, status,
                last_filled_quantity, filled_quantity, trade_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (event_time) DO NOTHING
            "#,
        );
        let _unused = sqlx::query(&query)
            .bind(data.event_time)
            .bind(data.side)
            .bind(data.order_type)
            .bind(data.time_in_force)
            .bind(data.quantity)
            .bind(data.price)
            .bind(data.average_price)
            .bind(data.status)
            .bind(data.last_filled_quantity)
            .bind(data.filled_quantity)
            .bind(data.trade_time)
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row_batch(&self, table_name: &str, data: Vec<ForceOrderRow>) -> Result<()> {
        let mut query_builder: sqlx::QueryBuilder<'_, sqlx::Postgres> = sqlx::QueryBuilder::new(
            format!(
                "INSERT INTO {table_name} (event_time, side, order_type, time_in_force, 
                quantity, price, average_price, status, last_filled_quantity, filled_quantity, trade_time) "
            ),
        );

        let _unused = query_builder
            .push_values(data, |mut b, row| {
                let _unused = b
                    .push_bind(row.event_time)
                    .push_bind(row.side)
                    .push_bind(row.order_type)
                    .push_bind(row.time_in_force)
                    .push_bind(row.quantity)
                    .push_bind(row.price)
                    .push_bind(row.average_price)
                    .push_bind(row.status)
                    .push_bind(row.last_filled_quantity)
                    .push_bind(row.filled_quantity)
                    .push_bind(row.trade_time);
            })
            .push(" ON CONFLICT (event_time) DO NOTHING")
            .build()
            .execute(&self.client)
            .await
            .context(error::PostgresSnafu)?;

        Ok(())
    }
}
