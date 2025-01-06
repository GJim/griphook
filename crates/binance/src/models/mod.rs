use serde::{Deserialize, Serialize};

pub mod agg_trade;
pub mod avg_price;
mod avro;
pub mod book_depth;
pub mod book_ticker;
pub mod kline;
pub mod mini_ticker;
pub mod partial_book_depth;
pub mod ticker;
pub mod trade;
pub mod window_ticker;

/// Represents a price and quantity pair in the order book
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Order {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
}

pub use agg_trade::AggTrade;
pub use avg_price::AvgPrice;
pub use avro::Avro;
pub use book_depth::BookDepth;
pub use book_ticker::BookTicker;
pub use kline::Kline;
pub use mini_ticker::MiniTicker;
pub use partial_book_depth::PartialBookDepth;
pub use ticker::Ticker;
pub use trade::Trade;
pub use window_ticker::WindowTicker;
