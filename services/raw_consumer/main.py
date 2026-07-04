import datetime
import gzip
import json
import logging

import boto3
from botocore.client import Config as BotoConfig

from common import config, mq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


s3 = get_s3_client()


def object_key_for(source: str, received_at: str, job_id: str) -> str:
    dt = datetime.datetime.fromisoformat(received_at)
    return f"{source}/{dt:%Y-%m-%d}/{dt:%H}/{job_id}.ndjson.gz"


def handle_message(payload: dict, channel) -> None:
    job_id = payload["job_id"]
    source = payload["source"]
    lines = payload["lines"]
    received_at = payload["received_at"]

    object_key = object_key_for(source, received_at, job_id)

    ndjson_body = (
        "\n".join(
            json.dumps({"line_no": i, "line": line, "source": source, "received_at": received_at})
            for i, line in enumerate(lines)
        )
        + "\n"
    ).encode("utf-8")

    s3.put_object(
        Bucket=config.MINIO_BUCKET,
        Key=object_key,
        Body=gzip.compress(ndjson_body),
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )

    mq.publish_json(
        channel,
        config.TRUSTED_QUEUE,
        {
            "job_id": job_id,
            "source": source,
            "object_key": object_key,
            "line_count": len(lines),
        },
    )
    logger.info("wrote %s (%d lines)", object_key, len(lines))


if __name__ == "__main__":
    mq.consume_forever(config.RAW_QUEUE, handle_message)
