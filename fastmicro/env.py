import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "0"))
TIMEOUT = float(os.getenv("TIMEOUT", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
PULSAR_SERVICE_URL = os.getenv("SERVICE_URL", "pulsar://localhost:6650")
REDIS_ADDRESS = os.getenv("ADDRESS", "redis://localhost:6379")
