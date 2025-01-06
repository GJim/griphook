# Griphook

### Commands
* create a cexflow config file
```
cargo run --bin cexflow default-config > cexflow.yaml
```
* run binance stream service
```
cargo run --bin cexflow -- binance trade-stream
```
* inspect a topic
```
cargo run --bin cexflow -- binance inspect binance.btcusdt.ticker
```

## Kafka
* [confluentinc-config](https://github.com/confluentinc/cp-all-in-one/blob/7.8.0-post/cp-all-in-one-community/docker-compose.yml)
> remove `connect` `ksql-datagen`
* cmake on windows
> Microsoft Build Tools: Desktop development with C++

