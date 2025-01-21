use crate::{
    database::{ClickhouseRows, DatabaseManager, DatabaseMessage},
    error::Result,
    AggTradeRow, AvgPriceRow, BookDepthNestedRow, BookTickerRow, ContinuousKlineRow, ForceOrderRow,
    KlineRow, MarkPriceRow, MiniTickerRow, PartialBookDepthNestedRow, TickerRow, TradeRow,
    WindowTickerRow,
};
// use snafu::ResultExt;
use std::collections::HashSet;
use tokio::sync::{mpsc, RwLock};
use tokio_graceful_shutdown::SubsystemHandle;

pub struct ClickhouseManager {
    pub db: clickhouse::Client,
    pub table_cache: RwLock<HashSet<String>>,
}

impl ClickhouseManager {
    #[allow(dead_code)]
    #[must_use]
    pub fn new_client(db: clickhouse::Client) -> Self {
        Self::new(db)
    }
}

impl DatabaseManager for ClickhouseManager {
    type DB = clickhouse::Client;
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

    /// Run loop specialized for handling Clickhouse messages.
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
                            DatabaseMessage::Clickhouse(msg) => {
                                match msg.rows {
                                    ClickhouseRows::AggTrade(rows) => {
                                        self.ensure_table_exists::<AggTradeRow>(&msg.table_name).await?;
                                        self.batch_insert::<AggTradeRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::AvgPrice(rows) => {
                                        self.ensure_table_exists::<AvgPriceRow>(&msg.table_name).await?;
                                        self.batch_insert::<AvgPriceRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::BookDepth(rows) => {
                                        self.ensure_table_exists::<BookDepthNestedRow>(&msg.table_name).await?;
                                        self.batch_insert::<BookDepthNestedRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::BookTicker(rows) => {
                                        self.ensure_table_exists::<BookTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<BookTickerRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::ContinuousKline(rows) => {
                                        self.ensure_table_exists::<ContinuousKlineRow>(&msg.table_name).await?;
                                        self.batch_insert::<ContinuousKlineRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::ForceOrder(rows) => {
                                        self.ensure_table_exists::<ForceOrderRow>(&msg.table_name).await?;
                                        self.batch_insert::<ForceOrderRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::Kline(rows) => {
                                        self.ensure_table_exists::<KlineRow>(&msg.table_name).await?;
                                        self.batch_insert::<KlineRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::MarkPrice(rows) => {
                                        self.ensure_table_exists::<MarkPriceRow>(&msg.table_name).await?;
                                        self.batch_insert::<MarkPriceRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::MiniTicker(rows) => {
                                        self.ensure_table_exists::<MiniTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<MiniTickerRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::PartialBookDepth(rows) => {
                                        self.ensure_table_exists::<PartialBookDepthNestedRow>(&msg.table_name)
                                            .await?;
                                        self.batch_insert::<PartialBookDepthNestedRow>(&msg.table_name, rows)
                                            .await?;
                                    }
                                    ClickhouseRows::Ticker(rows) => {
                                        self.ensure_table_exists::<TickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<TickerRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::Trade(rows) => {
                                        self.ensure_table_exists::<TradeRow>(&msg.table_name).await?;
                                        self.batch_insert::<TradeRow>(&msg.table_name, rows).await?;
                                    }
                                    ClickhouseRows::WindowTicker(rows) => {
                                        self.ensure_table_exists::<WindowTickerRow>(&msg.table_name).await?;
                                        self.batch_insert::<WindowTickerRow>(&msg.table_name, rows).await?;
                                    }
                                }
                            }
                            DatabaseMessage::Postgres(_) => {
                                tracing::warn!(
                                    "Clickhouse database manager does not support Postgres messages"
                                );
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
                            DatabaseMessage::Postgres(_) => {
                                tracing::warn!("Skipping Postgres message during shutdown");
                            }
                            DatabaseMessage::Clickhouse(msg) => {
                                // Use a shorter timeout for each operation during shutdown
                                let op_timeout = tokio::time::Duration::from_secs(1);

                                match msg.rows {
                                    ClickhouseRows::AggTrade(rows) => {
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
                                    ClickhouseRows::AvgPrice(rows) => {
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
                                    ClickhouseRows::BookDepth(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<BookDepthNestedRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<BookDepthNestedRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    ClickhouseRows::BookTicker(rows) => {
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
                                    ClickhouseRows::ContinuousKline(rows) => {
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
                                    ClickhouseRows::ForceOrder(rows) => {
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
                                    ClickhouseRows::Kline(rows) => {
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
                                    ClickhouseRows::MarkPrice(rows) => {
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
                                    ClickhouseRows::MiniTicker(rows) => {
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
                                    ClickhouseRows::PartialBookDepth(rows) => {
                                        if tokio::time::timeout(op_timeout, async {
                                            if let Err(e) = self.ensure_table_exists::<PartialBookDepthNestedRow>(&msg.table_name).await {
                                                tracing::error!("Error ensuring table exists during shutdown: {}", e);
                                                return;
                                            }
                                            if let Err(e) = self.batch_insert::<PartialBookDepthNestedRow>(&msg.table_name, rows).await {
                                                tracing::error!("Error inserting rows during shutdown: {}", e);
                                            }
                                        }).await == Ok(()) {
                                            continue;
                                        }
                                        tracing::warn!("Operation timeout during shutdown, skipping message");
                                    }
                                    ClickhouseRows::Ticker(rows) => {
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
                                    ClickhouseRows::Trade(rows) => {
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
                                    ClickhouseRows::WindowTicker(rows) => {
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

                    // Close the Clickhouse client gracefully
                    tracing::info!("Closing Clickhouse client...");
                    // Note: The Clickhouse client doesn't have an explicit close method,
                    // but we'll drop it naturally when the manager is dropped

                    tracing::info!("Finished processing remaining messages, shutting down");
                    return Ok(());
                }
            }
        }
    }
}
