mod ch;
mod pg;

use crate::{
    error::{self, Result},
    AggTradeRow, AvgPriceRow, BookDepthNestedRow, BookDepthRow, BookTickerRow, ContinuousKlineRow,
    ForceOrderRow, KlineRow, MarkPriceRow, MiniTickerRow, ModelsResult, PartialBookDepthNestedRow,
    PartialBookDepthRow, TickerRow, TradeRow, WindowTickerRow,
};
use snafu::ResultExt;
// use sqlx::{Pool, Postgres};
use std::collections::HashSet;
use tokio::sync::{mpsc, RwLock};
use tokio_graceful_shutdown::SubsystemHandle;

pub use ch::ClickhouseManager;
pub use pg::PostgresManager;

pub enum DatabaseType {
    Clickhouse,
    Postgres,
}

pub struct ClickhouseDB {
    pub client: clickhouse::Client,
}

impl ClickhouseDB {
    #[must_use]
    pub const fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

pub struct PostgresDB {
    pub client: sqlx::PgPool,
}

impl PostgresDB {
    #[must_use]
    pub const fn new(client: sqlx::PgPool) -> Self {
        Self { client }
    }
}

pub trait Database<T, U> {
    /// Ensures the table exists in the database.
    async fn ensure_table_exists(&self, table_name: &str) -> ModelsResult<()>;

    async fn to_row(&self, data: T) -> ModelsResult<U>;

    /// Inserts a row into the database.
    async fn insert_row(&self, table_name: &str, data: U) -> ModelsResult<()>;

    /// Inserts a row into the database.
    async fn insert_row_batch(&self, table_name: &str, data: Vec<U>) -> ModelsResult<()>;
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

// pub struct DatabaseManager<T> {
//     pub db: T,
//     pub table_cache: RwLock<HashSet<String>>,
// }

// impl DatabaseManager<clickhouse::Client> {
//     #[must_use]
//     pub fn new(db: clickhouse::Client) -> Self {
//         Self { db, table_cache: RwLock::new(HashSet::new()) }
//     }

//     async fn ensure_table_exists<T: Storage<clickhouse::Client>>(
//         &self,
//         table_name: &str,
//     ) -> Result<()> {
//         if !self
//             .table_cache
//             .try_read()
//             .context(error::CacheTryLockSnafu)?
//             .contains(&table_name.to_string())
//         {
//             T::ensure_table_exists(&self.db, table_name).await.context(error::ModelSnafu)?;
//             let mut cache_writer =
//                 self.table_cache.try_write().context(error::CacheTryLockSnafu)?;
//             let _unused = cache_writer.insert(table_name.to_string());
//         }
//         Ok(())
//     }

//     async fn batch_insert<T: Storage<clickhouse::Client>>(
//         &self,
//         table_name: &str,
//         data: Vec<T>,
//     ) -> Result<()> {
//         T::batch_insert(&self.db, table_name, data).await.context(error::ModelSnafu)
//     }

//     #[allow(clippy::missing_errors_doc)]
//     pub async fn run(
//         &self,
//         mut rx: mpsc::Receiver<DatabaseMessage>,
//         subsys: SubsystemHandle,
//     ) -> Result<()> {
//         loop {
//             tokio::select! {
//                 msg_opt = rx.recv() => {
//                     if let Some(msg) = msg_opt {
//                         match msg {
//                             DatabaseMessage::Clickhouse(msg) => match msg.rows {
//                                 ClickhouseRows::AggTrade(rows) => {
//                                     self.ensure_table_exists::<AggTradeRow>(&msg.table_name).await?;
//                                     self.batch_insert::<AggTradeRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::AvgPrice(rows) => {
//                                     self.ensure_table_exists::<AvgPriceRow>(&msg.table_name).await?;
//                                     self.batch_insert::<AvgPriceRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::BookDepth(rows) => {
//                                     self.ensure_table_exists::<BookDepthNestedRow>(&msg.table_name).await?;
//                                     self.batch_insert::<BookDepthNestedRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::BookTicker(rows) => {
//                                     self.ensure_table_exists::<BookTickerRow>(&msg.table_name).await?;
//                                     self.batch_insert::<BookTickerRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::ContinuousKline(rows) => {
//                                     self.ensure_table_exists::<ContinuousKlineRow>(&msg.table_name).await?;
//                                     self.batch_insert::<ContinuousKlineRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::ForceOrder(rows) => {
//                                     self.ensure_table_exists::<ForceOrderRow>(&msg.table_name).await?;
//                                     self.batch_insert::<ForceOrderRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::Kline(rows) => {
//                                     self.ensure_table_exists::<KlineRow>(&msg.table_name).await?;
//                                     self.batch_insert::<KlineRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::MarkPrice(rows) => {
//                                     self.ensure_table_exists::<MarkPriceRow>(&msg.table_name).await?;
//                                     self.batch_insert::<MarkPriceRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::MiniTicker(rows) => {
//                                     self.ensure_table_exists::<MiniTickerRow>(&msg.table_name).await?;
//                                     self.batch_insert::<MiniTickerRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::PartialBookDepth(rows) => {
//                                     self.ensure_table_exists::<PartialBookDepthNestedRow>(&msg.table_name)
//                                         .await?;
//                                     self.batch_insert::<PartialBookDepthNestedRow>(&msg.table_name, rows)
//                                         .await?;
//                                 }
//                                 ClickhouseRows::Ticker(rows) => {
//                                     self.ensure_table_exists::<TickerRow>(&msg.table_name).await?;
//                                     self.batch_insert::<TickerRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::Trade(rows) => {
//                                     self.ensure_table_exists::<TradeRow>(&msg.table_name).await?;
//                                     self.batch_insert::<TradeRow>(&msg.table_name, rows).await?;
//                                 }
//                                 ClickhouseRows::WindowTicker(rows) => {
//                                     self.ensure_table_exists::<WindowTickerRow>(&msg.table_name).await?;
//                                     self.batch_insert::<WindowTickerRow>(&msg.table_name, rows).await?;
//                                 }
//                             },
//                             DatabaseMessage::Postgres(_) => {
//                                 tracing::warn!(
//                                     "Clickhouse database manager does not support Postgres messages"
//                                 );
//                             }
//                         }
//                     }
//                 }
//                 () = subsys.on_shutdown_requested() => {
//                     tracing::info!("Shutdown requested");
//                     return Ok(());
//                 }
//             }
//         }
//     }
// }

// impl DatabaseManager<Pool<Postgres>> {
//     #[must_use]
//     pub fn new(db: Pool<Postgres>) -> Self {
//         Self { db, table_cache: RwLock::new(HashSet::new()) }
//     }

//     async fn ensure_table_exists<T: Storage<Pool<Postgres>>>(
//         &self,
//         table_name: &str,
//     ) -> Result<()> {
//         if !self
//             .table_cache
//             .try_read()
//             .context(error::CacheTryLockSnafu)?
//             .contains(&table_name.to_string())
//         {
//             T::ensure_table_exists(&self.db, table_name).await.context(error::ModelSnafu)?;
//             let mut cache_writer =
//                 self.table_cache.try_write().context(error::CacheTryLockSnafu)?;
//             let _unused = cache_writer.insert(table_name.to_string());
//         }
//         Ok(())
//     }

//     async fn batch_insert<T: Storage<Pool<Postgres>>>(
//         &self,
//         table_name: &str,
//         data: Vec<T>,
//     ) -> Result<()> {
//         T::batch_insert(&self.db, table_name, data).await.context(error::ModelSnafu)
//     }

//     #[allow(clippy::missing_errors_doc)]
//     pub async fn run(
//         &self,
//         mut rx: mpsc::Receiver<DatabaseMessage>,
//         subsys: SubsystemHandle,
//     ) -> Result<()> {
//         loop {
//             tokio::select! {
//                     msg_opt = rx.recv() => {
//                         if let Some(msg) = msg_opt {
//                             match msg {
//                                 DatabaseMessage::Clickhouse(_) => {
//                                     tracing::warn!(
//                                         "Postgres database manager does not support Clickhouse messages"
//                                     );
//                                 }
//                                 DatabaseMessage::Postgres(msg) => match msg.rows {
//                                     PostgresRows::AggTrade(rows) => {
//                                         self.ensure_table_exists::<AggTradeRow>(&msg.table_name).await?;
//                                         self.batch_insert::<AggTradeRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::AvgPrice(rows) => {
//                                         self.ensure_table_exists::<AvgPriceRow>(&msg.table_name).await?;
//                                         self.batch_insert::<AvgPriceRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::BookDepth(rows) => {
//                                         self.ensure_table_exists::<BookDepthRow>(&msg.table_name).await?;
//                                         self.batch_insert::<BookDepthRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::BookTicker(rows) => {
//                                         self.ensure_table_exists::<BookTickerRow>(&msg.table_name).await?;
//                                         self.batch_insert::<BookTickerRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::ContinuousKline(rows) => {
//                                         self.ensure_table_exists::<ContinuousKlineRow>(&msg.table_name).await?;
//                                         self.batch_insert::<ContinuousKlineRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::ForceOrder(rows) => {
//                                         self.ensure_table_exists::<ForceOrderRow>(&msg.table_name).await?;
//                                         self.batch_insert::<ForceOrderRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::Kline(rows) => {
//                                         self.ensure_table_exists::<KlineRow>(&msg.table_name).await?;
//                                         self.batch_insert::<KlineRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::MarkPrice(rows) => {
//                                         self.ensure_table_exists::<MarkPriceRow>(&msg.table_name).await?;
//                                         self.batch_insert::<MarkPriceRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::MiniTicker(rows) => {
//                                         self.ensure_table_exists::<MiniTickerRow>(&msg.table_name).await?;
//                                         self.batch_insert::<MiniTickerRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::PartialBookDepth(rows) => {
//                                         self.ensure_table_exists::<PartialBookDepthRow>(&msg.table_name).await?;
//                                         self.batch_insert::<PartialBookDepthRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::Ticker(rows) => {
//                                         self.ensure_table_exists::<TickerRow>(&msg.table_name).await?;
//                                         self.batch_insert::<TickerRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::Trade(rows) => {
//                                         self.ensure_table_exists::<TradeRow>(&msg.table_name).await?;
//                                         self.batch_insert::<TradeRow>(&msg.table_name, rows).await?;
//                                     }
//                                     PostgresRows::WindowTicker(rows) => {
//                                         self.ensure_table_exists::<WindowTickerRow>(&msg.table_name).await?;
//                                         self.batch_insert::<WindowTickerRow>(&msg.table_name, rows).await?;
//                                     }
//                                 }
//                             }
//                         }
//                     }
//                 () = subsys.on_shutdown_requested() => {
//                     tracing::info!("Shutdown requested");
//                     return Ok(());
//                 }
//             }
//         }
//     }
// }
