# Griphook

Griphook is a powerful toolkit for cryptocurrency trading and data analysis. It provides a robust infrastructure for connecting to cryptocurrency exchanges (currently focusing on Binance), streaming real-time market data, and storing it in various databases for analysis.

## Features

- **Real-time Data Streaming**: Connect to Binance WebSocket API to stream various market data types including trades, order books, klines, tickers, and more
- **Data Processing Pipeline**: Utilize Kafka for reliable message queuing and processing
- **Flexible Storage Options**: Store data in Clickhouse or PostgreSQL depending on your analytical needs
- **Command-line Interface**: Easy-to-use CLI for managing streams, inspecting data, and configuring the system
- **Multiple Trading Types**: Support for Spot, USD Futures, and COIN Futures markets
- **Configurable**: Extensive YAML-based configuration system

## Architecture

Griphook follows a modular architecture with the following components:
- **cexflow**: Main CLI application for interacting with exchange data flows
- **crates/base**: Core utilities and common functionality
- **crates/binance**: Binance-specific implementations for data streaming and processing

### Commands

- create a cexflow config file

```
cargo run --bin cexflow default-config > cexflow.yaml
```

- run binance stream service

```
cargo run --bin cexflow -- binance trade-stream
```

- inspect a topic

```
cargo run --bin cexflow -- binance inspect binance.btcusdt.ticker -o earliest
```

- sink a topic

```
cargo run --bin cexflow -- binance sink binance.btcusdt.ticker -o earliest -s postgres
```

## Kafka installation

- [confluentinc-config](https://github.com/confluentinc/cp-all-in-one/blob/7.8.0-post/cp-all-in-one-community/docker-compose.yml)
  > remove `connect` `ksql-datagen`
- cmake on windows
  > Microsoft Build Tools: Desktop development with C++
- cmake on linux
  > sudo apt-get install cmake

### Common commands

```shell
# list topics
kafka-topics --list --bootstrap-server localhost:9092

# delete topic
kafka-topics --delete --bootstrap-server localhost:9092 --topic binance.btcusdt.ticker

# list consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe

# delete consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group griphook
```

## Clickhouse installation

- Run Database service

```
docker run --name griphook-clickhouse --restart always -p 8123:8123 -p 9000:9000 -d clickhouse/clickhouse-server

docker run --name griphook-postgres --restart always -e POSTGRES_USER=username -e POSTGRES_PASSWORD=password -d -p 5432:5432 postgres
```

- Access Database service

```
docker exec -it griphook-clickhouse clickhouse-client -d griphook

docker exec -it griphook-postgres psql -U username -d griphook
```

- show all tables row counts

```sql
SELECT
    database,
    table,
    SUM(rows) AS row_count
FROM
    system.parts
WHERE
    active = 1
    AND database = 'griphook'
GROUP BY
    database, table
ORDER BY
    row_count DESC;
```

```sql
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS row_count
FROM
    pg_stat_user_tables
ORDER BY
    row_count DESC;
```