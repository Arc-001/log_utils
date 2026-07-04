import gzip
import json
import logging

import boto3
from botocore.client import Config as BotoConfig
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

from common import config, mq
from multiline import join_multiline

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


def build_template_miner() -> TemplateMiner:
    miner_config = TemplateMinerConfig()
    miner_config.drain_sim_th = 0.4
    miner_config.drain_depth = 4
    miner_config.drain_max_children = 100
    miner_config.drain_max_clusters = 1000
    # In-memory only for phase 3 tuning; template persistence to Postgres
    # (with LLM-generated regex + validation) lands in phase 4.
    return TemplateMiner(config=miner_config)


s3 = get_s3_client()
template_miner = build_template_miner()


def fetch_lines(object_key: str) -> list[str]:
    obj = s3.get_object(Bucket=config.MINIO_BUCKET, Key=object_key)
    body = gzip.decompress(obj["Body"].read())
    records = [json.loads(line) for line in body.decode("utf-8").splitlines() if line]
    records.sort(key=lambda r: r["line_no"])
    return [r["line"] for r in records]


def handle_message(payload: dict, channel) -> None:
    object_key = payload["object_key"]
    source = payload["source"]

    raw_lines = fetch_lines(object_key)
    entries = join_multiline(raw_lines)

    for entry in entries:
        result = template_miner.add_log_message(entry)
        logger.info(
            "source=%s change=%s cluster_id=%s cluster_size=%s template=%r",
            source,
            result.get("change_type"),
            result.get("cluster_id"),
            result.get("cluster_size"),
            result.get("template_mined"),
        )

    logger.info(
        "processed %s: %d raw lines -> %d logical entries, %d total clusters so far",
        object_key,
        len(raw_lines),
        len(entries),
        len(template_miner.drain.clusters),
    )


if __name__ == "__main__":
    mq.consume_forever(config.TRUSTED_QUEUE, handle_message)
