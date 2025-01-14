use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse f64 from string: {source}, location: {location}"))]
    ParseF64 {
        source: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse json: {source}, location: {location}"))]
    ParseJson {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ClickHouse error: {source}, location: {location}"))]
    Clickhouse {
        source: clickhouse::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("PostgreSQL error: {source}, location: {location}"))]
    Postgres {
        source: sqlx::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
