"""Bitquery Kafka consumer for EVM prediction-market trades.

Subscribes to a `*.predictions.proto` topic (default: matic.predictions.proto,
which carries Polymarket trades), decodes each PredictionMarketBlockMessage,
emits one CSV row per PredictionTradeEvent with block time, system receive
time, and end-to-end latency, and on shutdown logs:

  * total trade count
  * first trade (block number + block time)
  * last  trade (block number + block time + system receive time)
  * average / p90 / p99 / max latency (max(0, received_time - block_time))

The headline latency metric is `max(0, received_time - block_time)` —
the gap between when the Polygon block was minted and when our process
saw the trade, clamped at 0 (block_time is integer seconds, so a fast
pipeline + validator clock skew can otherwise produce small negatives).
The Kafka producer timestamp is still recorded in the CSV
(`kafka_ts_iso`, `latency_kafka_sec`) for debugging.

Schema reference:
  https://github.com/bitquery/streaming_protobuf/blob/main/evm/prediction_market_block_message.proto
Streaming concepts:
  https://docs.bitquery.io/docs/streams/kafka-streaming-concepts/
"""
from __future__ import annotations

import csv
import datetime as dt
import logging
import os
import signal
import threading
import time
import uuid
from typing import List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf import text_format
from google.protobuf.message import DecodeError

# pb2 bindings provided by the `bitquery-pb2-kafka-package` PyPI package,
# which installs `evm/`, `solana/`, etc. at top-level on sys.path.
from evm import (
    dex_block_message_pb2 as dex_pb2,
    dex_pool_block_message_pb2 as dexpool_pb2,
    parsed_abi_block_message_pb2 as tx_pb2,
    prediction_market_block_message_pb2 as pm_pb2,
    token_block_message_pb2 as token_pb2,
)

import config


# Map a topic-suffix name to its top-level protobuf message class.
SCHEMAS = {
    "dextrades": dex_pb2.DexBlockMessage,
    "predictions": pm_pb2.PredictionMarketBlockMessage,
    "transactions": tx_pb2.ParsedAbiBlockMessage,
    "tokens": token_pb2.TokenBlockMessage,
    "dexpools": dexpool_pb2.DexPoolBlockMessage,
}


def schema_from_topic(topic: str) -> str:
    """Return the schema key (e.g. 'predictions') from a topic like
    'matic.predictions.proto' or 'matic.broadcasted.predictions.proto'.
    """
    parts = topic.split(".")
    if len(parts) >= 2 and parts[-1] == "proto":
        return parts[-2]
    return ""


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("predictions")


# --- helpers ---------------------------------------------------------------


def block_number_from_bytes(b: bytes) -> int:
    """EVM block number is a big-endian byte string."""
    return int.from_bytes(b, "big") if b else 0


def hex0x(b: bytes) -> str:
    return "0x" + b.hex() if b else ""


def block_time_to_iso(secs: int) -> str:
    if not secs:
        return ""
    return dt.datetime.fromtimestamp(secs, tz=dt.timezone.utc).isoformat()


def tx_time_to_iso(ns: int) -> str:
    """TransactionHeader.Time is nanoseconds (matches evm.go's txTimeString)."""
    if not ns:
        return ""
    return dt.datetime.fromtimestamp(ns / 1e9, tz=dt.timezone.utc).isoformat()


def percentile(sorted_values: List[float], p: float) -> float:
    """Nearest-rank percentile on a pre-sorted list. p in [0, 100]."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = max(0, min(len(sorted_values) - 1,
                   int(round((p / 100.0) * (len(sorted_values) - 1)))))
    return sorted_values[k]


# --- state -----------------------------------------------------------------


class Stats:
    """Running stats over every prediction trade we observe.

    Latency here = max(0, received_time - block_time) (i.e. how long
    after the Polygon block was minted our process saw the trade). This
    is the end-to-end "chain to us" latency. Note: block_time has
    1-second granularity, so individual values can be off by up to ~1s
    in either direction; raw negative values (a fast pipeline + validator
    clock skew) are clamped to 0.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.total = 0
        self.latencies: List[float] = []  # seconds, received - block_time
        self.first_block: Optional[int] = None
        self.first_block_time_iso: str = ""
        self.last_block: Optional[int] = None
        self.last_block_time_iso: str = ""
        self.last_received_iso: str = ""
        # Highest-latency trade we've seen so far. Use -inf because block-time
        # latency can be slightly negative (1s block-time truncation + clock
        # skew), and we still want to pick the largest of those if it's all
        # we have.
        self.max_latency_sec: float = float("-inf")
        self.max_latency_trade: Optional[dict] = None
        # Wall-clock start of the run (set when Stats is constructed).
        self.start_unix: float = time.time()
        self.start_iso: str = dt.datetime.fromtimestamp(
            self.start_unix, tz=dt.timezone.utc
        ).isoformat()

    def record(self, block_number: int, block_time_iso: str,
               received_iso: str, latency_sec: Optional[float],
               trade_ref: Optional[dict] = None) -> None:
        with self.lock:
            self.total += 1
            if latency_sec is not None:
                self.latencies.append(latency_sec)
                if latency_sec > self.max_latency_sec:
                    self.max_latency_sec = latency_sec
                    self.max_latency_trade = trade_ref
            if self.first_block is None:
                self.first_block = block_number
                self.first_block_time_iso = block_time_iso
            self.last_block = block_number
            self.last_block_time_iso = block_time_iso
            self.last_received_iso = received_iso

    def snapshot(self) -> dict:
        with self.lock:
            lat = sorted(self.latencies)
            avg = (sum(lat) / len(lat)) if lat else 0.0
            now_unix = time.time()
            return {
                "total": self.total,
                "first_block": self.first_block,
                "first_block_time": self.first_block_time_iso,
                "last_block": self.last_block,
                "last_block_time": self.last_block_time_iso,
                "last_received": self.last_received_iso,
                "avg_latency_sec": avg,
                "p90_latency_sec": percentile(lat, 90),
                "p99_latency_sec": percentile(lat, 99),
                "max_latency_sec": (
                    self.max_latency_sec
                    if self.max_latency_sec != float("-inf") else 0.0
                ),
                "max_latency_trade": self.max_latency_trade,
                "start_iso": self.start_iso,
                "elapsed_sec": now_unix - self.start_unix,
            }


def _fmt_duration(seconds: float) -> str:
    """Format a duration as 'Hh Mm Ss' (or 'Mm Ss' / 'Ss' when shorter)."""
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def log_summary(stats: Stats, label: str = "running") -> None:
    s = stats.snapshot()

    if label == "final":
        # Multi-line, one detail per line for readability at shutdown.
        m = s["max_latency_trade"]
        lines = [
            "==================== FINAL SUMMARY ====================",
            f"  run started      : {s['start_iso']}",
            f"  ran for          : {_fmt_duration(s['elapsed_sec'])} "
            f"({s['elapsed_sec']:.3f}s)",
            f"  total trades     : {s['total']}",
            f"  first block      : {s['first_block']}",
            f"  first block time : {s['first_block_time']}",
            f"  last block       : {s['last_block']}",
            f"  last block time  : {s['last_block_time']}",
            f"  last received    : {s['last_received']}",
            "  --- latency (received - block_time) ---",
            f"  avg latency      : {s['avg_latency_sec']:.3f}s",
            f"  p90 latency      : {s['p90_latency_sec']:.3f}s",
            f"  p99 latency      : {s['p99_latency_sec']:.3f}s",
            f"  max latency      : {s['max_latency_sec']:.3f}s",
        ]
        if m:
            lines += [
                "  --- max-latency trade ---",
                f"  block            : {m['block']}",
                f"  tx_index         : {m['tx_index']}",
                f"  tx_hash          : {m['tx_hash']}",
                f"  block_time       : {m['block_time_iso']}",
                f"  received         : {m['received_iso']}",
                f"  latency          : {m['latency_sec']:.3f}s",
            ]
        lines.append("=" * 55)
        for line in lines:
            log.info(line)
        return

    # Compact single-line "running" summary.
    log.info(
        "[%s summary] total_trades=%d  ran_for=%s  "
        "first_block=%s @ %s  last_block=%s @ %s  last_received=%s  "
        "block_latency: avg=%.3fs p90=%.3fs p99=%.3fs max=%.3fs",
        label,
        s["total"],
        _fmt_duration(s["elapsed_sec"]),
        s["first_block"], s["first_block_time"],
        s["last_block"], s["last_block_time"],
        s["last_received"],
        s["avg_latency_sec"], s["p90_latency_sec"], s["p99_latency_sec"],
        s["max_latency_sec"],
    )
    if s["max_latency_trade"]:
        m = s["max_latency_trade"]
        log.info(
            "  max-latency trade: block=%s tx_index=%s tx_hash=%s "
            "block_time=%s received=%s latency=%.3fs",
            m["block"], m["tx_index"], m["tx_hash"],
            m["block_time_iso"], m["received_iso"], m["latency_sec"],
        )


# --- CSV writer ------------------------------------------------------------


CSV_FIELDS = [
    "block_number",
    "block_time_iso",
    "block_time_unix",
    "tx_index",
    "tx_hash",
    "tx_time_iso",
    "log_index",
    "call_index",
    "event_type",
    "protocol",
    "protocol_family",
    "market_id",
    "question_title",
    "question_id",
    "condition_id",
    "outcome_index",
    "outcome_label",
    "outcome_token_id",
    "buyer",
    "seller",
    "is_outcome_buy",
    "amount_raw",
    "collateral_amount_raw",
    "order_id",
    # All times are UTC. There are three "when?" timestamps per trade:
    #   block_time     - validator-set, integer seconds (low resolution)
    #   kafka_ts       - when Bitquery's producer published the message
    #   received_time  - when our process consumed the message
    # And two latencies derived from them:
    #   latency_block_sec  = max(0, received - block_time)  (clamped to 0;
    #                          block_time is 1s-granular so raw value can be
    #                          slightly negative on a fast pipeline)
    #   latency_kafka_sec  = received - kafka_ts            (always >= 0)
    "kafka_ts_iso",
    "kafka_ts_unix",
    "received_time_iso",
    "received_time_unix",
    "latency_block_sec",
    "latency_kafka_sec",
]


# --- main loop -------------------------------------------------------------


def build_consumer() -> Consumer:
    group_id = f"{config.username}-predictions-{uuid.uuid4().hex[:8]}"
    conf = {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": group_id,
        "session.timeout.ms": 30000,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": config.username,
        "sasl.password": config.password,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    log.info("connecting to %s as %s (group=%s, topic=%s)",
             config.bootstrap_servers, config.username, group_id, config.topic)
    c = Consumer(conf)
    c.subscribe([config.topic])
    return c


def handle_block(msg, writer: csv.DictWriter, stats: Stats) -> None:
    batch = pm_pb2.PredictionMarketBlockMessage()
    try:
        batch.ParseFromString(msg.value())
    except DecodeError as e:
        log.error("protobuf decode failed: %s", e)
        return

    block_number = block_number_from_bytes(batch.Header.Number)
    block_time_unix = int(batch.Header.Time)  # seconds
    block_time_iso = block_time_to_iso(block_time_unix)

    if not batch.TradeEvents:
        return

    # Kafka producer timestamp (ms since epoch). Same value for every trade in
    # this batch — they share one Kafka message.
    ts_type, ts_ms = msg.timestamp()
    kafka_ts_unix = (ts_ms / 1000.0) if ts_ms and ts_ms > 0 else 0.0
    kafka_ts_iso = (
        dt.datetime.fromtimestamp(kafka_ts_unix, tz=dt.timezone.utc).isoformat()
        if kafka_ts_unix else ""
    )

    for event in batch.TradeEvents:
        received_unix = time.time()
        received_iso = dt.datetime.fromtimestamp(
            received_unix, tz=dt.timezone.utc
        ).isoformat()

        # Clamp at 0: block_time is integer seconds, so a fast pipeline can
        # produce a small "negative" latency purely from the 1-second
        # truncation + validator clock skew. We treat those as "0s" so the
        # number always means "this many seconds (or less) after the block
        # was minted".
        latency_block = (
            max(0.0, received_unix - block_time_unix)
            if block_time_unix else None
        )
        latency_kafka = (
            received_unix - kafka_ts_unix if kafka_ts_unix else None
        )

        tx_hdr = event.TransactionHeader
        pm_event = event.Event
        prediction = event.Prediction
        trade = event.OutcomeTrade
        marketplace = prediction.Marketplace
        question = prediction.Question
        condition = prediction.Condition
        outcome = prediction.Outcome  # optional

        tx_hash_hex = hex0x(tx_hdr.Hash)

        row = {
            "block_number": block_number,
            "block_time_iso": block_time_iso,
            "block_time_unix": block_time_unix,
            "tx_index": event.TransactionIndex,
            "tx_hash": tx_hash_hex,
            "tx_time_iso": tx_time_to_iso(tx_hdr.Time),
            "log_index": pm_event.LogIndex,
            "call_index": pm_event.CallIndex,
            "event_type": pm_event.Type,
            "protocol": marketplace.ProtocolName,
            "protocol_family": marketplace.ProtocolFamily,
            "market_id": question.MarketId,
            "question_title": question.Title,
            "question_id": hex0x(question.Id),
            "condition_id": hex0x(condition.Id),
            "outcome_index": outcome.Index if outcome else "",
            "outcome_label": outcome.Label if outcome else "",
            "outcome_token_id": hex0x(outcome.Id) if outcome and outcome.Id else "",
            "buyer": hex0x(trade.Buyer),
            "seller": hex0x(trade.Seller),
            "is_outcome_buy": bool(trade.IsOutcomeBuy),
            "amount_raw": int.from_bytes(trade.Amount, "big") if trade.Amount else 0,
            "collateral_amount_raw": (
                int.from_bytes(trade.CollateralAmount, "big")
                if trade.CollateralAmount else 0
            ),
            "order_id": hex0x(trade.OrderId),
            "kafka_ts_iso": kafka_ts_iso,
            "kafka_ts_unix": f"{kafka_ts_unix:.6f}" if kafka_ts_unix else "",
            "received_time_iso": received_iso,
            "received_time_unix": f"{received_unix:.6f}",
            "latency_block_sec": (
                f"{latency_block:.6f}" if latency_block is not None else ""
            ),
            "latency_kafka_sec": (
                f"{latency_kafka:.6f}" if latency_kafka is not None else ""
            ),
        }
        writer.writerow(row)

        trade_ref = {
            "block": block_number,
            "block_time_iso": block_time_iso,
            "tx_index": event.TransactionIndex,
            "tx_hash": tx_hash_hex,
            "received_iso": received_iso,
            "latency_sec": latency_block if latency_block is not None else 0.0,
        }
        stats.record(
            block_number=block_number,
            block_time_iso=block_time_iso,
            received_iso=received_iso,
            latency_sec=latency_block,
            trade_ref=trade_ref,
        )


def dump_message(msg, dump_cls) -> None:
    """Decode a Kafka message with the given pb2 class and pretty-print it."""
    obj = dump_cls()
    try:
        obj.ParseFromString(msg.value())
    except DecodeError as e:
        log.error("decode failed for %s as %s: %s",
                  msg.topic(), dump_cls.__name__, e)
        return
    print("=" * 80)
    print(f"topic={msg.topic()} partition={msg.partition()} "
          f"offset={msg.offset()} key={msg.key()!r} "
          f"kafka_ts={msg.timestamp()} schema={dump_cls.__name__}")
    print("-" * 80)
    print(text_format.MessageToString(obj, as_utf8=True))


def main() -> None:
    shutdown = threading.Event()

    def _signal_handler(signum, _frame):
        log.info("received signal %s, shutting down...", signum)
        shutdown.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    stats = Stats()
    last_summary = time.time()

    # Resolve dump mode (debug only).
    dump_cls = None
    dump_count = 0
    if config.dump_messages or config.dump_schema:
        schema_key = config.dump_schema or schema_from_topic(config.topic)
        dump_cls = SCHEMAS.get(schema_key)
        if dump_cls is None:
            log.error("unknown dump schema %r for topic %r. "
                      "Set BITQUERY_DUMP_SCHEMA to one of: %s",
                      schema_key, config.topic, ", ".join(sorted(SCHEMAS)))
            return
        log.info("DUMP MODE: decoding every message as %s "
                 "(max=%s, CSV/stats disabled)",
                 dump_cls.__name__, config.dump_max or "unlimited")

    # Only open the CSV when not in dump mode.
    csv_file = None
    writer = None
    if dump_cls is None:
        csv_exists = os.path.exists(config.csv_path) and os.path.getsize(config.csv_path) > 0
        csv_file = open(config.csv_path, "a", newline="", buffering=1)
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        if not csv_exists:
            writer.writeheader()
        log.info("writing trades to %s", os.path.abspath(config.csv_path))

    consumer = build_consumer()

    try:
        while not shutdown.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                err = msg.error()
                code = err.code()
                if code == KafkaError._PARTITION_EOF:
                    pass
                elif code == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    log.error(
                        "topic authorization failed for %s — your Bitquery "
                        "account is not entitled to read this topic. "
                        "Ask Bitquery support/sales to grant access for user "
                        "%r, or set BITQUERY_TOPIC to a topic you do have.",
                        config.topic, config.username,
                    )
                    shutdown.set()
                else:
                    raise KafkaException(err)
            else:
                if dump_cls is not None:
                    dump_message(msg, dump_cls)
                    dump_count += 1
                    if config.dump_max and dump_count >= config.dump_max:
                        log.info("dump_max=%d reached, exiting", config.dump_max)
                        shutdown.set()
                else:
                    try:
                        handle_block(msg, writer, stats)
                    except Exception:
                        log.exception("failed to process message at %s[%d]@%s",
                                      msg.topic(), msg.partition(), msg.offset())

            if dump_cls is None and \
               time.time() - last_summary >= config.summary_interval_sec:
                log_summary(stats, label="running")
                last_summary = time.time()

    except KeyboardInterrupt:
        log.info("keyboard interrupt")
    finally:
        log.info("closing consumer...")
        try:
            consumer.close()
        finally:
            if csv_file is not None:
                csv_file.flush()
                csv_file.close()
        if dump_cls is None:
            log_summary(stats, label="final")
        else:
            log.info("dump mode: %d messages printed", dump_count)


if __name__ == "__main__":
    main()
