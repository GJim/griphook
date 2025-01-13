use clickhouse::Row;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::{
    error::{self, Result},
    Avro,
};
use crate::{ClickhouseDB, Database, PostgresDB};

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

impl Database<Trade, TradeRow> for ClickhouseDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {table_name} (
                    trade_id Int64,
                    price Float64,
                    quantity Float64,
                    trade_time Int64,
                    is_buyer_market_maker Bool
                )
                ENGINE = MergeTree()
                ORDER BY (trade_id)
                "#
        );
        self.client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Trade) -> Result<TradeRow> {
        Ok(TradeRow {
            trade_id: data.trade_id,
            price: data.price.parse().context(error::ParseF64Snafu)?,
            quantity: data.quantity.parse().context(error::ParseF64Snafu)?,
            trade_time: data.trade_time,
            is_buyer_market_maker: data.is_buyer_market_maker,
        })
    }

    /// Inserts a row into the database.
    async fn insert_row(&self, table_name: &str, data: TradeRow) -> Result<()> {
        let mut insert =
            self.client.insert::<TradeRow>(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}

impl Database<Trade, TradeRow> for PostgresDB {
    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
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
            sqlx::query(&create_table).execute(&self.client).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn to_row(&self, data: Trade) -> Result<TradeRow> {
        Ok(TradeRow {
            trade_id: data.trade_id,
            price: data.price.parse().context(error::ParseF64Snafu)?,
            quantity: data.quantity.parse().context(error::ParseF64Snafu)?,
            trade_time: data.trade_time,
            is_buyer_market_maker: data.is_buyer_market_maker,
        })
    }

    /// Inserts a row into the database.
    async fn insert_row(&self, table_name: &str, data: TradeRow) -> Result<()> {
        let _unused = sqlx::query(&format!(
            "
                INSERT INTO {table_name} 
                    (trade_id, price, quantity, trade_time, is_buyer_market_maker) 
                VALUES ($1, $2, $3, $4, $5) 
                ON CONFLICT (trade_id) 
                DO NOTHING
            "
        ))
        .bind(data.trade_id)
        .bind(data.price)
        .bind(data.quantity)
        .bind(data.trade_time)
        .bind(data.is_buyer_market_maker)
        .bind(table_name)
        .execute(&self.client)
        .await
        .context(error::PostgresSnafu)?;
        Ok(())
    }
}

// impl ToDatabase<ClickHouse> for Trade {
//     type Row = TradeRow;

//     fn create_table_sql(table_name: &str) -> String {
//         return format!(
//             r#"
//             CREATE TABLE IF NOT EXISTS {table_name} (
//                 trade_id Int64,
//                 price Float64,
//                 quantity Float64,
//                 trade_time Int64,
//                 is_buyer_market_maker Bool
//             )
//             ENGINE = MergeTree()
//             ORDER BY (trade_id)
//             "#
//         );
//     }

//     fn to_row(self) -> Result<Self::Row> {
//         Ok(TradeRow {
//             trade_id: self.trade_id,
//             price: self.price.parse().context(error::ParseF64Snafu)?,
//             quantity: self.quantity.parse().context(error::ParseF64Snafu)?,
//             trade_time: self.trade_time,
//             is_buyer_market_maker: self.is_buyer_market_maker,
//         })
//     }
// }

// impl ToDatabase<Postgres> for Trade {
//     type Row = String;

//     fn create_table_sql(table_name: &str) -> String {
//         return format!(
//             r#"
//             CREATE TABLE IF NOT EXISTS {table_name} (
//                 trade_id BIGINT,
//                 price DOUBLE PRECISION,
//                 quantity DOUBLE PRECISION,
//                 trade_time BIGINT,
//                 is_buyer_market_maker BOOLEAN,
//                 PRIMARY KEY (trade_id)
//             )
//             "#
//         );
//     }

//     fn to_row(self) -> Result<Self::Row> {
//         let query = format!(
//             "INSERT INTO {} (trade_id, price, quantity, trade_time, is_buyer_market_maker) \
//              VALUES ($1, $2, $3, $4, $5) ON CONFLICT (trade_id) DO UPDATE SET price = $2, quantity = $3, trade_time = $4, is_buyer_market_maker = $5",
//             table_name
//         );
//         Ok(TradeRow {
//             trade_id: self.trade_id,
//             price: self.price.parse().context(error::ParseF64Snafu)?,
//             quantity: self.quantity.parse().context(error::ParseF64Snafu)?,
//             trade_time: self.trade_time,
//             is_buyer_market_maker: self.is_buyer_market_maker,
//         })
//     }
// }
