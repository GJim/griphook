use serde::{Deserialize, Serialize};
use snafu::ResultExt;

pub mod agg_trade;
pub mod avg_price;
mod avro;
pub mod book_depth;
pub mod book_ticker;
pub mod continuous_kline;
pub mod error;
pub mod force_order;
pub mod kline;
pub mod mark_price;
pub mod mini_ticker;
pub mod partial_book_depth;
pub mod ticker;
pub mod trade;
pub mod window_ticker;

pub use agg_trade::{AggTrade, AggTradeRow};
pub use avg_price::{AvgPrice, AvgPriceRow};
pub use avro::Avro;
pub use book_depth::{BookDepth, BookDepthNestedRow, BookDepthRow};
pub use book_ticker::{BookTicker, BookTickerRow};
pub use continuous_kline::{ContinuousKline, ContinuousKlineRow};
pub use force_order::{ForceOrder, ForceOrderRow};
pub use kline::{Kline, KlineRow};
pub use mark_price::{MarkPrice, MarkPriceRow};
pub use mini_ticker::{MiniTicker, MiniTickerRow};
pub use partial_book_depth::{PartialBookDepth, PartialBookDepthNestedRow, PartialBookDepthRow};
pub use ticker::{Ticker, TickerRow};
pub use trade::{Trade, TradeRow};
pub use window_ticker::{WindowTicker, WindowTickerRow};

/// Represents a price and quantity pair in the order book
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Order {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
}

#[derive(clickhouse::Row, Serialize, sqlx::FromRow)]
pub struct OrderRow {
    pub price: f64,
    pub quantity: f64,
}

impl TryFrom<Order> for OrderRow {
    type Error = error::Error;

    fn try_from(data: Order) -> error::Result<Self> {
        Ok(Self {
            price: data.price.parse().context(error::ParseF64Snafu)?,
            quantity: data.quantity.parse().context(error::ParseF64Snafu)?,
        })
    }
}

#[allow(dead_code)]
pub trait ClickhouseRow {
    type Row: clickhouse::Row + Serialize;

    async fn insert_row(
        client: &clickhouse::Client,
        table_name: &str,
        data: Self::Row,
    ) -> error::Result<()> {
        let mut insert = client.insert(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)
    }

    async fn insert_row_batch(
        client: &clickhouse::Client,
        table_name: &str,
        data: Vec<Self::Row>,
    ) -> error::Result<()> {
        let mut insert = client.insert(table_name).context(error::ClickhouseSnafu)?;
        for row in data {
            insert.write(&row).await.context(error::ClickhouseSnafu)?;
        }
        insert.end().await.context(error::ClickhouseSnafu)
    }
}
