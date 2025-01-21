mod clickhouse_manager;
mod postgres_manager;

use crate::{
    error::{self, Result},
    AggTradeRow, AvgPriceRow, BookDepthNestedRow, BookDepthRow, BookTickerRow, ContinuousKlineRow,
    ForceOrderRow, KlineRow, MarkPriceRow, MiniTickerRow, ModelsResult, PartialBookDepthNestedRow,
    PartialBookDepthRow, TickerRow, TradeRow, WindowTickerRow,
};
use snafu::ResultExt;
use std::collections::HashSet;
use tokio::sync::{mpsc, RwLock};
use tokio_graceful_shutdown::SubsystemHandle;

pub use clickhouse_manager::ClickhouseManager;
pub use postgres_manager::PostgresManager;

pub enum DatabaseType {
    Clickhouse,
    Postgres,
}

pub trait Storage<T>
where
    Self: Sized,
{
    async fn ensure_table_exists(client: &T, table_name: &str) -> ModelsResult<()>;

    async fn batch_insert(client: &T, table_name: &str, data: Vec<Self>) -> ModelsResult<()>;
}

pub enum DatabaseMessage {
    Clickhouse(ClickhouseMessage),
    Postgres(PostgresMessage),
}

pub struct ClickhouseMessage {
    pub table_name: String,
    pub rows: ClickhouseRows,
}

pub enum ClickhouseRows {
    AggTrade(Vec<AggTradeRow>),
    AvgPrice(Vec<AvgPriceRow>),
    BookDepth(Vec<BookDepthNestedRow>),
    BookTicker(Vec<BookTickerRow>),
    ContinuousKline(Vec<ContinuousKlineRow>),
    ForceOrder(Vec<ForceOrderRow>),
    Kline(Vec<KlineRow>),
    MarkPrice(Vec<MarkPriceRow>),
    MiniTicker(Vec<MiniTickerRow>),
    PartialBookDepth(Vec<PartialBookDepthNestedRow>),
    Ticker(Vec<TickerRow>),
    Trade(Vec<TradeRow>),
    WindowTicker(Vec<WindowTickerRow>),
}

pub struct PostgresMessage {
    pub table_name: String,
    pub rows: PostgresRows,
}

pub enum PostgresRows {
    AggTrade(Vec<AggTradeRow>),
    AvgPrice(Vec<AvgPriceRow>),
    BookDepth(Vec<BookDepthRow>),
    BookTicker(Vec<BookTickerRow>),
    ContinuousKline(Vec<ContinuousKlineRow>),
    ForceOrder(Vec<ForceOrderRow>),
    Kline(Vec<KlineRow>),
    MarkPrice(Vec<MarkPriceRow>),
    MiniTicker(Vec<MiniTickerRow>),
    PartialBookDepth(Vec<PartialBookDepthRow>),
    Ticker(Vec<TickerRow>),
    Trade(Vec<TradeRow>),
    WindowTicker(Vec<WindowTickerRow>),
}

pub trait DatabaseManager: Send + Sync + 'static {
    /// The concrete database client type (e.g. `clickhouse::Client` or `Pool<Postgres>`).
    type DB;

    /// The type of incoming messages that this manager handles (`DatabaseMessage` in your case).
    type Message;

    /// Creates a new instance of the manager.
    fn new(db: Self::DB) -> Self;

    /// Returns a reference to the underlying DB client.
    fn db(&self) -> &Self::DB;

    /// Returns a reference to the table cache.
    fn table_cache(&self) -> &RwLock<HashSet<String>>;

    /// Ensures a table exists, using `Storage::ensure_table_exists`.
    async fn ensure_table_exists<T>(&self, table_name: &str) -> Result<()>
    where
        T: Storage<Self::DB> + Send + Sync,
    {
        // Try a read lock first.
        if !self.table_cache().try_read().context(error::CacheTryLockSnafu)?.contains(table_name) {
            // If not in cache, attempt creation.
            T::ensure_table_exists(self.db(), table_name).await.context(error::ModelSnafu)?;

            // Insert into cache with a write lock.
            let mut cache_writer =
                self.table_cache().try_write().context(error::CacheTryLockSnafu)?;
            let _ = cache_writer.insert(table_name.to_string());
        }
        Ok(())
    }

    /// Inserts a batch of rows into the given table, using `Storage::batch_insert`.
    async fn batch_insert<T>(&self, table_name: &str, data: Vec<T>) -> Result<()>
    where
        T: Storage<Self::DB> + Send + Sync,
    {
        T::batch_insert(self.db(), table_name, data).await.context(error::ModelSnafu)
    }

    /// Main run loop, consuming messages from the channel.
    async fn run(&self, rx: mpsc::Receiver<Self::Message>, subsys: SubsystemHandle) -> Result<()>;
}
