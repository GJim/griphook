use serde::{Deserialize, Serialize};
use snafu::ResultExt;

mod consumer;
mod error;
mod models;
mod producer;

pub use consumer::inspector;
pub use error::Error;
use error::{AvroSerializationSnafu, Result};
pub use models::{
    AggTrade, AvgPrice, Avro, BookDepth, BookTicker, Kline, MiniTicker, PartialBookDepth, Ticker,
    Trade, WindowTicker,
};
pub use producer::run;

const CEX_TOPIC_PREFIX: &str = "binance";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventMessage {
    #[serde(rename = "stream")]
    pub stream: String,
    #[serde(rename = "data")]
    pub data: Event,
}

impl EventMessage {
    #[allow(clippy::missing_errors_doc)]
    pub fn topic(&self) -> Result<String> {
        let parts: Vec<&str> = self.stream.split('@').collect();
        if parts.len() < 2 {
            return Err(Error::UnsupportedStreamType { stream_type: self.stream.clone() });
        }
        let symbol = parts[0].to_lowercase();
        let stream_type = parts[1].to_lowercase();
        Ok(format!("{CEX_TOPIC_PREFIX}.{symbol}.{stream_type}"))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Event {
    AggTrade(AggTrade),
    AvgPrice(AvgPrice),
    BookDepth(BookDepth),
    BookTicker(BookTicker),
    Kline(Kline),
    PartialBookDepth(PartialBookDepth),
    Trade(Trade),
    // Deserialize order: [ticker > window_ticker > mini_ticker]
    Ticker(Ticker),
    WindowTicker(WindowTicker),
    MiniTicker(MiniTicker),
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
        }
    }
}
