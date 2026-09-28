"""
StockFlow configuration management.

Centralizes all environment variables and application settings.
Uses python-dotenv to load from .env file.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# === Finnhub API ===
FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_WS_URL: str = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# === Kafka ===
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRADES: str = "finnhub_realtime_trades"

# === AWS S3 ===
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "stockflow-lakehouse")
S3_REGION: str = os.getenv("S3_REGION", "ap-southeast-1")

# === Symbols to track ===
# Default watchlist - can be overridden via environment variable
_default_symbols = "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META"
WATCHLIST: list[str] = os.getenv("WATCHLIST", _default_symbols).split(",")

# === PostgreSQL (Gold Layer) ===
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "stockflow_gold")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "stockflow")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "stockflow2026")
POSTGRES_URL: str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
