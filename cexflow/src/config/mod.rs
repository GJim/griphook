mod binance;
mod clickhouse;
pub mod error;
mod kafka;
mod log;
mod postgres;

use clap::ValueEnum;
use resolve_path::PathResolveExt;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::path::{Path, PathBuf};

use griphook_binance::database::DatabaseType;

pub use self::error::{Error, Result};
use self::{binance::BinanceConfig, kafka::KafkaConfig, log::LogConfig};

#[derive(Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum Storage {
    Clickhouse,
    Postgres,
}

impl AsRef<str> for Storage {
    fn as_ref(&self) -> &str {
        match self {
            Self::Clickhouse => "clickhouse",
            Self::Postgres => "postgres",
        }
    }
}

impl std::fmt::Display for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl From<Storage> for DatabaseType {
    fn from(value: Storage) -> Self {
        match value {
            Storage::Clickhouse => Self::Clickhouse,
            Storage::Postgres => Self::Postgres,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub log: LogConfig,

    #[serde(default)]
    pub kafka: KafkaConfig,

    #[serde(default)]
    pub binance: BinanceConfig,

    #[serde(default)]
    pub clickhouse: clickhouse::ClickhouseConfig,

    #[serde(default)]
    pub postgres: postgres::PostgresConfig,
}

#[allow(dead_code)]
impl Config {
    pub fn search_config_file_path() -> PathBuf {
        let paths = vec![Self::default_path()]
            .into_iter()
            .chain(griphook_base::fallback_project_config_directories().into_iter().map(
                |mut path| {
                    path.push(griphook_base::CEX_CONFIG_FILE_NAME);
                    path
                },
            ))
            .collect::<Vec<_>>();
        for path in paths {
            let Ok(exists) = path.try_exists() else {
                continue;
            };
            if exists {
                return path;
            }
        }
        Self::default_path()
    }

    #[inline]
    pub fn default_path() -> PathBuf {
        [
            griphook_base::PROJECT_CONFIG_DIR.to_path_buf(),
            PathBuf::from(griphook_base::CEX_CONFIG_FILE_NAME),
        ]
        .into_iter()
        .collect()
    }

    #[inline]
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut config: Self = {
            let data = std::fs::read_to_string(&path)
                .context(error::OpenConfigSnafu { filename: path.as_ref().to_path_buf() })?;

            serde_yaml::from_str(&data)
                .context(error::ParseConfigSnafu { filename: path.as_ref().to_path_buf() })?
        };

        config.log.file_path = match config.log.file_path.map(|path| {
            path.try_resolve()
                .map(|path| path.to_path_buf())
                .with_context(|_| error::ResolveFilePathSnafu { file_path: path.clone() })
        }) {
            Some(Ok(path)) => Some(path),
            Some(Err(err)) => return Err(err),
            None => None,
        };

        Ok(config)
    }
}
