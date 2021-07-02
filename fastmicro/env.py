import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "0"))
TIMEOUT = float(os.getenv("TIMEOUT", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_ADDRESS = os.getenv("REDIS_ADDRESS", "redis://localhost:6379")
