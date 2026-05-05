"""Bitquery Kafka credentials & topic config.

Credentials and other settings are loaded from a local `.env` file (see
`.env.example`). Values can also be overridden via real environment
variables: BITQUERY_USERNAME, BITQUERY_PASSWORD, BITQUERY_TOPIC, etc.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {name!r}. "
            "Set it in your .env file (see .env.example)."
        )
    return value


username = _required("BITQUERY_USERNAME")
password = _required("BITQUERY_PASSWORD")

bootstrap_servers = os.getenv(
    "BITQUERY_BOOTSTRAP",
    "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
)

# Polygon prediction-markets topic (Polymarket lives here).
# Use "matic.broadcasted.predictions.proto" for mempool-level (lower latency, unconfirmed).
topic = os.getenv("BITQUERY_TOPIC", "matic.predictions.proto")

# Where to write the per-trade CSV.
csv_path = os.getenv("BITQUERY_CSV", "prediction_trades.csv")

# How often (seconds) to print a running summary to the log.
summary_interval_sec = int(os.getenv("BITQUERY_SUMMARY_INTERVAL", "30"))

# DEBUG: if truthy, decode every Kafka message with the schema appropriate for
# `topic` (auto-detected from the topic name) and print the full protobuf
# contents to stdout instead of writing to CSV. Override the auto-detected
# schema with BITQUERY_DUMP_SCHEMA = one of:
#   dextrades | predictions | transactions | tokens | dexpools
dump_messages = os.getenv("BITQUERY_DUMP", "").lower() in ("1", "true", "yes")
dump_schema = os.getenv("BITQUERY_DUMP_SCHEMA", "")  # "" = auto-detect from topic
dump_max = int(os.getenv("BITQUERY_DUMP_MAX", "0"))  # 0 = unlimited
