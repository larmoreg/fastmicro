import os

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "0"))
TIMEOUT = float(os.getenv("TIMEOUT", "0"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222")
NATS_CLUSTER_ID = os.getenv("NATS_CLUSTER_ID", "test-cluster")
NATS_CLIENT_ID = os.getenv("NATS_CLIENT_ID", "test-client")
REDIS_ADDRESS = os.getenv("REDIS_ADDRESS", "redis://localhost:6379")
