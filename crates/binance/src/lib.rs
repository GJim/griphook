use serde::{Deserialize, Serialize};
use snafu::ResultExt;

mod consumer;
pub mod database;
mod error;
mod models;
pub mod producer;

pub use consumer::{inspector, sink::Sink};
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

#[derive(Debug)]
pub enum StreamType {
    AggTrade,
    AvgPrice,
    BookDepth,
    BookTicker,
    ContinuousKline,
    Kline,
    ForceOrder,
    MarkPrice,
    MiniTicker,
    PartialBookDepth,
    Ticker,
    Trade,
    WindowTicker,
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AggTrade => write!(f, "aggtrade"),
            Self::AvgPrice => write!(f, "avgprice"),
            Self::BookDepth => write!(f, "depth"),
            Self::BookTicker => write!(f, "bookticker"),
            Self::ContinuousKline => write!(f, "continuouskline"),
            Self::Kline => write!(f, "kline"),
            Self::ForceOrder => write!(f, "forceorder"),
            Self::MarkPrice => write!(f, "markprice"),
            Self::MiniTicker => write!(f, "miniticker"),
            Self::PartialBookDepth => write!(f, "partialbookdepth"),
            Self::Ticker => write!(f, "ticker"),
            Self::Trade => write!(f, "trade"),
            Self::WindowTicker => write!(f, "windowticker"),
        }
    }
}

impl TryFrom<&String> for StreamType {
    type Error = Error;

    fn try_from(value: &String) -> Result<Self> {
        match extract_stream_type_from_topic(value)?.as_str() {
            "aggtrade" => Ok(Self::AggTrade),
            "trade" => Ok(Self::Trade),
            "avgprice" => Ok(Self::AvgPrice),
            "depth" => Ok(Self::BookDepth),
            "bookticker" => Ok(Self::BookTicker),
            "kline" => Ok(Self::Kline),
            "miniticker" => Ok(Self::MiniTicker),
            "partialbookdepth" => Ok(Self::PartialBookDepth),
            "ticker" => Ok(Self::Ticker),
            "windowticker" => Ok(Self::WindowTicker),
            "forceorder" => Ok(Self::ForceOrder),
            "markprice" => Ok(Self::MarkPrice),
            "continuouskline" => Ok(Self::ContinuousKline),
            _ => Err(Error::InvalidStreamTopic { topic: value.to_string() }),
        }
    }
}

fn extract_stream_type_from_topic(input: &str) -> Result<String> {
    let parts: Vec<&str> = input.split('.').collect();

    if parts.len() > 3 {
        let stream_type = parts[3].to_lowercase();
        if stream_type.contains("ticker_") {
            return Ok("windowticker".to_string());
        } else if stream_type.contains("depth") && stream_type.len() > 5 {
            return Ok("partialbookdepth".to_string());
        } else if stream_type.contains("continuouskline_") {
            return Ok("continuouskline".to_string());
        } else if stream_type.contains("kline") {
            return Ok("kline".to_string());
        }
        Ok(stream_type)
    } else {
        Err(Error::InvalidStreamTopic { topic: input.to_string() })
    }
}
