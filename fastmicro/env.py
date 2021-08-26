import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "0"))
MESSAGING_TIMEOUT = float(os.getenv("MESSAGING_TIMEOUT", "0"))
PROCESSING_TIMEOUT = (
    float(os.environ["PROCESSING_TIMEOUT"]) if "PROCESSING_TIMEOUT" in os.environ else None
)
RESENDS = int(os.getenv("RESENDS", "0"))
RETRIES = int(os.getenv("RETRIES", "0"))
SLEEP_TIME = float(os.getenv("SLEEP_TIME", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222")
NATS_CLUSTER_ID = os.getenv("NATS_CLUSTER_ID", "test-cluster")
NATS_CLIENT_ID = os.getenv("NATS_CLIENT_ID", "test-client")
PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
REDIS_ADDRESS = os.getenv("REDIS_ADDRESS", "redis://localhost:6379")
