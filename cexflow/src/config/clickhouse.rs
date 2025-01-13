use clickhouse::Client;
use griphook_base::PROJECT_NAME;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClickhouseConfig {
    #[serde(default = "ClickhouseConfig::default_url")]
    pub url: String,

    #[serde(default = "ClickhouseConfig::default_database")]
    pub database: String,

    #[serde(default = "ClickhouseConfig::default_username")]
    pub username: Option<String>,

    #[serde(default = "ClickhouseConfig::default_password")]
    pub password: Option<String>,

    #[serde(default = "ClickhouseConfig::default_compression")]
    pub compression: bool,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            url: Self::default_url(),
            database: Self::default_database(),
            username: Self::default_username(),
            password: Self::default_password(),
            compression: Self::default_compression(),
        }
    }
}

impl ClickhouseConfig {
    #[inline]
    #[must_use]
    pub fn default_url() -> String {
        "http://localhost:8123".to_string()
    }

    #[inline]
    #[must_use]
    pub fn default_database() -> String {
        PROJECT_NAME.to_string()
    }

    #[inline]
    #[must_use]
    pub const fn default_username() -> Option<String> {
        None
    }

    #[inline]
    #[must_use]
    pub const fn default_password() -> Option<String> {
        None
    }

    #[inline]
    #[must_use]
    pub const fn default_compression() -> bool {
        true
    }

    pub fn create_client(&self) -> Client {
        let mut client = Client::default().with_url(&self.url).with_database(&self.database);

        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            client = client.with_user(username).with_password(password);
        }

        if self.compression {
            client = client.with_compression(clickhouse::Compression::Lz4);
        }

        client
    }
}
