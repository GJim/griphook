use crate::ModelsResult;

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
