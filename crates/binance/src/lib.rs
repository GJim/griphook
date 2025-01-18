use serde::{Deserialize, Serialize};
use snafu::ResultExt;

mod consumer;
pub mod database;
mod error;
mod models;
pub mod producer;

pub use consumer::{inspector, sink::Consumer};
pub use error::Error;
use error::{AvroSerializationSnafu, Result};
pub use models::{
    error::Result as ModelsResult, AggTrade, AggTradeRow, AvgPrice, AvgPriceRow, Avro, BookDepth,
    BookDepthNestedRow, BookDepthRow, BookTicker, BookTickerRow, ContinuousKline,
    ContinuousKlineRow, ForceOrder, ForceOrderRow, Kline, KlineRow, MarkPrice, MarkPriceRow,
    MiniTicker, MiniTickerRow, PartialBookDepth, PartialBookDepthNestedRow, PartialBookDepthRow,
    Ticker, TickerRow, Trade, TradeRow, WindowTicker, WindowTickerRow,
};

const CEX_NAME: &str = "binance";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventMessage {
    #[serde(rename = "stream")]
    pub stream: String,
    #[serde(rename = "data")]
    pub data: Event,
}

impl EventMessage {
    #[allow(clippy::missing_errors_doc)]
    pub fn topic(&self, trading_type: &str) -> Result<String> {
        let parts: Vec<&str> = self.stream.split('@').collect();
        if parts.len() < 2 {
            return Err(Error::UnsupportedStreamType { stream_type: self.stream.clone() });
        }
        let symbol = parts[0].to_lowercase();
        let stream_type = parts[1].to_lowercase();
        Ok(format!("{CEX_NAME}.{trading_type}.{symbol}.{stream_type}"))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Event {
    AggTrade(AggTrade),
    AvgPrice(AvgPrice),
    BookDepth(BookDepth),
    BookTicker(BookTicker),
    ContinuousKline(ContinuousKline),
    Kline(Kline),
    ForceOrder(ForceOrder),
    PartialBookDepth(PartialBookDepth),
    Trade(Trade),
    // Deserialize order: [ticker > window_ticker > mini_ticker]
    Ticker(Ticker),
    WindowTicker(WindowTicker),
    MiniTicker(MiniTicker),
    MarkPrice(MarkPrice),
}

impl Event {
    #[must_use]
    const fn sequence_id(&self) -> [u8; 8] {
        let event_time = match self {
            Self::AggTrade(e) => e.event_time,
            Self::AvgPrice(e) => e.event_time,
            Self::BookDepth(e) => e.event_time,
            Self::BookTicker(e) => e.update_id,
            Self::Kline(e) => e.event_time,
            Self::MiniTicker(e) => e.event_time,
            Self::PartialBookDepth(e) => e.last_update_id,
            Self::Ticker(e) => e.event_time,
            Self::Trade(e) => e.event_time,
            Self::WindowTicker(e) => e.event_time,
            Self::ForceOrder(e) => e.event_time,
            Self::MarkPrice(e) => e.event_time,
            Self::ContinuousKline(e) => e.event_time,
        };

        event_time.to_le_bytes()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn to_compressed_message(&self) -> Result<Vec<u8>> {
        match self {
            Self::AggTrade(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::AvgPrice(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::BookDepth(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::BookTicker(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::Kline(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::MiniTicker(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::PartialBookDepth(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::Ticker(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::Trade(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::WindowTicker(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::ForceOrder(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::MarkPrice(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
            Self::ContinuousKline(e) => e.serialize_to_avro().context(AvroSerializationSnafu),
        }
    }
}
