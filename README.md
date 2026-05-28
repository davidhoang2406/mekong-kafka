# mekong-kafka

Producers and consumers that bridge upstream market-data APIs and MinIO via
Apache Kafka. This repo owns the **raw data in** path.

```
vnstock / Binance
  ├── stock-price-producer  ─┐
  └── crypto-price-producer ─┴→ Kafka (*.price.realtime)
                                  └── storage-consumer → MinIO (price.snapshot/*.avro)
```

## Components

```
producers/
  stock_price_producer.py   # vnstock polling → stock.price.realtime
  crypto_price_producer.py  # CCXT/Binance polling → crypto.price.realtime
  utils.py                  # symbol-list loaders, asset_class helper
consumers/
  storage_consumer.py       # batch Kafka → MinIO Avro, with DLQ
model/
  base_producer.py          # KafkaProducer wrapper (Confluent wire format)
  base_consumer.py          # KafkaConsumer wrapper
  minio_store.py            # MinIO write abstraction
db/                         # one-shot scripts to init / flush MinIO buckets
config/                     # stocks.json, crypto.json (symbol lists, intervals)
main.py                     # CLI entry — see below
```

## Run locally

```bash
cp .env.example .env        # adjust if not using default minikube stack
make install                # creates .venv, installs requirements.txt
make run-stock-price-producer
make run-crypto-price-producer
make run-storage-consumer
```

## CLI

```
python main.py <command>
```

| Command | Purpose |
|---|---|
| `smoke-producer` | Send one hardcoded message to `stock.price.realtime` |
| `smoke-consumer` | Print messages on `stock.price.realtime` |
| `stock-price-producer` | Poll vnstock every 30 s, publish to Kafka |
| `crypto-price-producer` | Poll Binance every 5 s, publish to Kafka |
| `storage-consumer` | Drain both topics, write Avro batches to MinIO |

## Environment

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `minioadmin` | MinIO credentials |
| `MINIO_BUCKET` | `market-data` | Raw Avro bucket |

## MinIO layout written

```
market-data/
  price.snapshot/asset_class={stock|crypto}/symbol={SYM}/year=/month=/day=/part-{ts}.avro
  dead-letter/{event_type}/{ts}.json     # malformed messages
  _SUCCESS/year=/month=/day=             # daily marker, written by ohlcv ingest
```

## Tests

```bash
make test            # all tests
make test-unit       # unit-only (no Docker)
make test-integration # requires running Kafka + MinIO
```

## Docker image

Built and pushed by CI:

```
ghcr.io/davidhoang2406/mekong-kafka:latest
```

Consumed by the three deployments in `mekong-infra/k8s/mekong-pipeline/`.

## Depends on

- [`mekong-data-models`](https://github.com/davidhoang2406/mekong-data-models) — topic constants + Avro schemas
- `mekong-infra` — provides Kafka, Schema Registry, MinIO
