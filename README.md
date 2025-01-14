# Griphook

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

## Kafka

- [confluentinc-config](https://github.com/confluentinc/cp-all-in-one/blob/7.8.0-post/cp-all-in-one-community/docker-compose.yml)
  > remove `connect` `ksql-datagen`
- cmake on windows
  > Microsoft Build Tools: Desktop development with C++

## Clickhouse

- Run Database service

```
docker run --name griphook-clickhouse -p 8123:8123 -p 9000:9000 -d clickhouse/clickhouse-server

docker run --name griphook-postgres -e POSTGRES_USER=username -e POSTGRES_PASSWORD=password -d -p 5432:5432 postgres
```

- Access Database service

```
docker exec -it griphook-clickhouse clickhouse-client -d griphook

docker exec -it griphook-postgres psql -U username -d griphook
```
