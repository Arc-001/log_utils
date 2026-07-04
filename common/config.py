import os

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASS = os.environ["RABBITMQ_PASS"]

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw-logs")

EXCHANGE = "log.events"
RAW_QUEUE = "raw.jobs"
TRUSTED_QUEUE = "trusted.jobs"
REFINED_QUEUE = "refined.jobs"
