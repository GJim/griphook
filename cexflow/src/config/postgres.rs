use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use crate::config::error;
use griphook_base::PROJECT_NAME;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresConfig {
    #[serde(default = "PostgresConfig::default_host")]
    pub host: String,

    #[serde(default = "PostgresConfig::default_port")]
    pub port: u16,

    #[serde(default = "PostgresConfig::default_db_name")]
    pub db_name: String,

    #[serde(default = "PostgresConfig::default_username")]
    pub username: String,

    #[serde(default = "PostgresConfig::default_password")]
    pub password: String,

    #[serde(default = "PostgresConfig::default_max_pool_size")]
    pub max_pool_size: u32,
}

impl PostgresConfig {
    /// Create a new postgres connection pool with the given configuration
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * Failed to create the connection pool
    /// * Invalid configuration parameters were provided
    pub async fn create_pool(&self) -> Result<Pool<Postgres>, error::Error> {
        let Self { username, password, host, port, db_name, max_pool_size } = self;

        let postgres_url = format!("postgres://{username}:{password}@{host}:{port}/{db_name}");

        let postgres_pool = PgPoolOptions::new()
            .max_connections(*max_pool_size)
            .connect(&postgres_url)
            .await
            .context(error::ConnectPostgresSnafu)?;

        Ok(postgres_pool)
    }

    #[inline]
    pub fn default_host() -> String {
        "localhost".to_string()
    }

    #[inline]
    pub const fn default_port() -> u16 {
        5432
    }

    #[inline]
    pub fn default_db_name() -> String {
        PROJECT_NAME.to_string()
    }

    #[inline]
    pub fn default_username() -> String {
        "username".to_string()
    }

    #[inline]
    pub fn default_password() -> String {
        "password".to_string()
    }

    #[inline]
    pub const fn default_max_pool_size() -> u32 {
        10
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
            db_name: Self::default_db_name(),
            username: Self::default_username(),
            password: Self::default_password(),
            max_pool_size: Self::default_max_pool_size(),
        }
    }
}
