# Polymarket prediction-trade Kafka consumer

Consumes Bitquery's Polygon prediction-market Kafka topic, decodes each
`PredictionMarketBlockMessage`, writes one CSV row per trade with the block
time, system receive time, and end-to-end latency, and on shutdown prints
total count, first/last trade markers, and avg / p90 / p99 latency.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The protobuf bindings come from the
[`bitquery-pb2-kafka-package`](https://pypi.org/project/bitquery-pb2-kafka-package/)
PyPI package — no local `.proto` files or `protoc` step needed.

## Configure

Set credentials (provided by Bitquery) either via env vars:

```bash
export BITQUERY_USERNAME='your_user'
export BITQUERY_PASSWORD='your_pass'
# optional overrides:
# export BITQUERY_TOPIC='matic.predictions.proto'             # committed
# export BITQUERY_TOPIC='matic.broadcasted.predictions.proto' # mempool
# export BITQUERY_CSV='prediction_trades.csv'
# export BITQUERY_SUMMARY_INTERVAL=30
```

…or edit `config.py` directly.

## Run

```bash
python consumer.py
```

Stop with Ctrl-C. The final summary is logged on shutdown.

## CSV columns

`block_number, block_time_iso, block_time_unix, tx_index, tx_hash,
tx_time_iso, log_index, call_index, event_type, protocol, protocol_family,
market_id, question_title, question_id, condition_id, outcome_index,
outcome_label, outcome_token_id, buyer, seller, is_outcome_buy, amount_raw,
collateral_amount_raw, order_id, kafka_ts_iso, kafka_ts_unix,
received_time_iso, received_time_unix, latency_block_sec, latency_kafka_sec`

There are three "when?" timestamps per trade (all UTC):

| column              | meaning                                                        |
| ------------------- | -------------------------------------------------------------- |
| `block_time_*`      | validator-set block timestamp, **integer seconds** resolution. |
| `kafka_ts_*`        | when Bitquery's producer published the message to Kafka.       |
| `received_time_*`   | when our process consumed the message.                         |

And two latencies derived from them:

- `latency_block_sec = received_time_unix - block_time_unix` — full
  block→consumer time. Can be **slightly negative** because block timestamps
  are truncated to whole seconds, so a fast pipeline can deliver a message
  before the rounded-up second tick.
- `latency_kafka_sec = received_time_unix - kafka_ts_unix` — pure Kafka
  pipeline latency (Bitquery published → we consumed). Always ≥ 0. **This is
  the headline number used for the avg / p90 / p99 / max summary.**

## References

- Schema: <https://github.com/bitquery/streaming_protobuf/blob/main/evm/prediction_market_block_message.proto>
- Streams docs: <https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/>
- Example consumer: <https://github.com/bitquery/streaming-protobuf-python/blob/main/consumer.py>
