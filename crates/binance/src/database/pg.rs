use crate::{
    database::{DatabaseManager, DatabaseMessage, PostgresRows},
    error::Result,
    AggTradeRow, AvgPriceRow, BookDepthRow, BookTickerRow, ContinuousKlineRow, ForceOrderRow,
    KlineRow, MarkPriceRow, MiniTickerRow, PartialBookDepthRow, TickerRow, TradeRow,
    WindowTickerRow,
};
// use snafu::ResultExt;
use std::collections::HashSet;
use tokio::sync::{mpsc, RwLock};
use tokio_graceful_shutdown::SubsystemHandle;

pub struct PostgresManager {
    db: sqlx::Pool<sqlx::Postgres>,
    table_cache: RwLock<HashSet<String>>,
}

impl PostgresManager {
    #[allow(dead_code)]
    #[must_use]
    pub fn new_pool(db: sqlx::Pool<sqlx::Postgres>) -> Self {
        Self::new(db)
    }
}

impl DatabaseManager for PostgresManager {
    type DB = sqlx::Pool<sqlx::Postgres>;
    type Message = DatabaseMessage;

    fn new(db: Self::DB) -> Self {
        Self { db, table_cache: RwLock::new(HashSet::new()) }
    }

    fn db(&self) -> &Self::DB {
        &self.db
    }

    fn table_cache(&self) -> &RwLock<HashSet<String>> {
        &self.table_cache
    }

    /// Run loop specialized for handling Postgres messages.
    #[allow(clippy::too_many_lines)]
    async fn run(
        &self,
        mut rx: mpsc::Receiver<Self::Message>,
        subsys: SubsystemHandle,
    ) -> Result<()> {
        loop {
            tokio::select! {
                msg_opt = rx.recv() => {
                    if let Some(msg) = msg_opt {
                        match msg {
                            // We ignore or warn about Clickhouse messages here:
                            DatabaseMessage::Clickhouse(_) => {
                                tracing::warn!(
                                    "Postgres database manager does not support Clickhouse messages"
                                );
                            }
                            DatabaseMessage::Postgres(msg) => {
                                match msg.rows {
                                    PostgresRows::AggTrade(rows) => {
                                        self.ensure_table_exists::<AggTradeRow>(&msg.table_name).await?;
                                        self.batch_insert::<AggTradeRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::AvgPrice(rows) => {
                                        self.ensure_table_exists::<AvgPriceRow>(&msg.table_name).await?;
                                        self.batch_insert::<AvgPriceRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::BookDepth(rows) => {
                                        self.ensure_table_exists::<BookDepthRow>(&msg.table_name).await?;
                                        self.batch_insert::<BookDepthRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::BookTicker(rows) => {
                                        self.ensure_table_exists::<BookTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<BookTickerRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::ContinuousKline(rows) => {
                                        self.ensure_table_exists::<ContinuousKlineRow>(&msg.table_name).await?;
                                        self.batch_insert::<ContinuousKlineRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::ForceOrder(rows) => {
                                        self.ensure_table_exists::<ForceOrderRow>(&msg.table_name).await?;
                                        self.batch_insert::<ForceOrderRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::Kline(rows) => {
                                        self.ensure_table_exists::<KlineRow>(&msg.table_name).await?;
                                        self.batch_insert::<KlineRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::MarkPrice(rows) => {
                                        self.ensure_table_exists::<MarkPriceRow>(&msg.table_name).await?;
                                        self.batch_insert::<MarkPriceRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::MiniTicker(rows) => {
                                        self.ensure_table_exists::<MiniTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<MiniTickerRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::PartialBookDepth(rows) => {
                                        self.ensure_table_exists::<PartialBookDepthRow>(&msg.table_name).await?;
                                        self.batch_insert::<PartialBookDepthRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::Ticker(rows) => {
                                        self.ensure_table_exists::<TickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<TickerRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::Trade(rows) => {
                                        self.ensure_table_exists::<TradeRow>(&msg.table_name).await?;
                                        self.batch_insert::<TradeRow>(&msg.table_name, rows).await?;
                                    }
                                    PostgresRows::WindowTicker(rows) => {
                                        self.ensure_table_exists::<WindowTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<WindowTickerRow>(&msg.table_name, rows).await?;
                                    }
                                }
                            }
                        }
                    } else {
                        tracing::info!("Channel closed, exiting");
                        return Ok(());
                    }
                }
                () = subsys.on_shutdown_requested() => {
                    tracing::info!("Shutdown requested, processing remaining messages...");

                    // Set a timeout for processing remaining messages (5 seconds)
                    let shutdown_timeout = tokio::time::Duration::from_secs(5);
                    let shutdown_deadline = tokio::time::Instant::now() + shutdown_timeout;

                    // Collect remaining messages with timeout
                    let mut pending_msgs = Vec::new();
                    while let Ok(msg) = rx.try_recv() {
                        pending_msgs.push(msg);
                        if tokio::time::Instant::now() >= shutdown_deadline {
                            tracing::warn!("Shutdown timeout reached after collecting {} messages", pending_msgs.len());
                            break;
                        }
                    }

                    tracing::info!("Processing {} remaining messages", pending_msgs.len());

                    // Process collected messages
                    for msg in pending_msgs {
                        match msg {
                            DatabaseMessage::Clickhouse(_) => {
                                tracing::warn!("Skipping Clickhouse message during shutdown");
                            }
                            DatabaseMessage::Postgres(msg) => {
                                // Use a shorter timeout for each operation during shutdown
                                let op_timeout = tokio::time::Duration::from_secs(1);

                                match msg.rows {
                                    PostgresRows::AggTrade(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<AggTradeRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<AggTradeRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::AvgPrice(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<AvgPriceRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<AvgPriceRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::BookDepth(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<BookDepthRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<BookDepthRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::BookTicker(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<BookTickerRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<BookTickerRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::ContinuousKline(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<ContinuousKlineRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<ContinuousKlineRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::ForceOrder(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<ForceOrderRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<ForceOrderRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::Kline(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<KlineRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<KlineRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::MarkPrice(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<MarkPriceRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<MarkPriceRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::MiniTicker(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<MiniTickerRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<MiniTickerRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::PartialBookDepth(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<PartialBookDepthRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<PartialBookDepthRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::Ticker(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<TickerRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<TickerRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::Trade(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<TradeRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<TradeRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    PostgresRows::WindowTicker(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<WindowTickerRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<WindowTickerRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                }
                            }
                        }
                    }

                    // Close the database connection pool gracefully
                    tracing::info!("Closing database connection pool...");
                    self.db.close().await;

                    tracing::info!("Finished processing remaining messages, shutting down");
                    return Ok(());
                }
            }
        }
    }
}
