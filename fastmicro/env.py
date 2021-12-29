import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))
CLIENT_TIMEOUT = (
    float(os.environ["CLIENT_TIMEOUT"]) if "CLIENT_TIMEOUT" in os.environ else None
)
MESSAGING_TIMEOUT = (
    float(os.environ["MESSAGING_TIMEOUT"])
    if "MESSAGING_TIMEOUT" in os.environ
    else None
)
SERVER_TIMEOUT = (
    float(os.environ["SERVER_TIMEOUT"]) if "SERVER_TIMEOUT" in os.environ else None
)
RAISES = "RAISES" in os.environ
RESENDS = int(os.getenv("RESENDS", "0"))
RETRIES = int(os.getenv("RETRIES", "0"))
SLEEP_TIME = float(os.getenv("SLEEP_TIME", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222")
NATS_CLUSTER_ID = os.getenv("NATS_CLUSTER_ID", "test-cluster")
NATS_CLIENT_ID = os.getenv("NATS_CLIENT_ID", "test-client")
PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
REDIS_ADDRESS = os.getenv("REDIS_ADDRESS", "redis://localhost:6379")
