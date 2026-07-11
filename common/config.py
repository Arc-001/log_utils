import os

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASS = os.environ["RABBITMQ_PASS"]

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw-logs")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "log_utils")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "log_utils")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "openrouter/auto")
TEMPLATE_SAMPLE_THRESHOLD = int(os.environ.get("TEMPLATE_SAMPLE_THRESHOLD", "5"))
TEMPLATE_VALIDATION_THRESHOLD = float(os.environ.get("TEMPLATE_VALIDATION_THRESHOLD", "0.8"))

EXCHANGE = "log.events"
RAW_QUEUE = "raw.jobs"
TRUSTED_QUEUE = "trusted.jobs"
REFINED_QUEUE = "refined.jobs"
