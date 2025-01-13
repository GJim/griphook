use crate::{
    error::{self, Result},
    ClickHouse, Database, ToDatabase,
};
use clickhouse::Client;
use serde::Serialize;
use snafu::ResultExt;
use std::marker::PhantomData;

pub struct ClickHouseDatabase<T>
where
    T: ToDatabase<ClickHouse>,
    T::Row: clickhouse::Row + Serialize,
{
    client: Client,
    _phantom: PhantomData<T>,
}

impl<T> ClickHouseDatabase<T>
where
    T: ToDatabase<ClickHouse>,
    T::Row: clickhouse::Row + Serialize,
{
    #[must_use]
    pub const fn new(client: Client) -> Self {
        Self { client, _phantom: PhantomData }
    }
}

impl<T> Database for ClickHouseDatabase<T>
where
    T: ToDatabase<ClickHouse>,
    T::Row: clickhouse::Row + Serialize,
{
    type Row = T::Row;

    async fn ensure_table_exists(&self, table_name: &str) -> Result<()> {
        let create_table = T::create_table_sql(table_name);
        self.client.query(&create_table).execute().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }

    async fn insert_row(&self, table_name: &str, data: Self::Row) -> Result<()> {
        let mut insert =
            self.client.insert::<Self::Row>(table_name).context(error::ClickhouseSnafu)?;
        insert.write(&data).await.context(error::ClickhouseSnafu)?;
        insert.end().await.context(error::ClickhouseSnafu)?;
        Ok(())
    }
}
