use crate::{
    error::{self, Result},
    Database, Postgres, ToDatabase,
};
use serde::Serialize;
use snafu::ResultExt;
use sqlx::PgPool;
use std::marker::PhantomData;

pub struct PostgresDatabase<T>
where
    T: ToDatabase<Postgres>,
{
    pool: PgPool,
    _phantom: PhantomData<T>,
}

impl<T> PostgresDatabase<T>
where
    T: ToDatabase<Postgres>,
{
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool, _phantom: PhantomData }
    }
}

impl<T> Database for PostgresDatabase<T>
where
    T: ToDatabase<Postgres>,
{
    type Row = T::Row;

    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = T::create_table_sql(table_name);
        sqlx::query(&create_table).execute(&self.pool).await.context(error::PostgresSnafu)?;
        Ok(())
    }

    async fn insert_row(&self, table_name: &str, data: Self::Row) -> Result<()> {
        let query = format!(
            "INSERT INTO {} (trade_id, price, quantity, trade_time, is_buyer_market_maker) \
             VALUES ($1, $2, $3, $4, $5)",
            table_name
        );

        match &data {
            TradeRow { trade_id, price, quantity, trade_time, is_buyer_market_maker } => {
                sqlx::query(&query)
                    .bind(trade_id)
                    .bind(price)
                    .bind(quantity)
                    .bind(trade_time)
                    .bind(is_buyer_market_maker)
                    .execute(&self.pool)
                    .await
                    .context(error::PostgresSnafu)?;
            }
        }

        Ok(())
    }
}
