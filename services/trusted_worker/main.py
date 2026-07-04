import gzip
import hashlib
import json
import logging
import re

import boto3
from botocore.client import Config as BotoConfig
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

import llm
import templates_store
from common import config, mq
from multiline import join_multiline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Regex that compiles but never matches anything - placeholder for
# templates whose generation failed outright (network/LLM error), so
# they still satisfy the NOT NULL constraint and land in status=review.
NEVER_MATCHES_REGEX = "(?!)"

# Tracks per (source, drain3 cluster_id) state: either still buffering
# samples, or resolved to a persisted template. Keyed by cluster_id
# rather than the mined template text, because that text mutates every
# time Drain3 generalizes the cluster further (cluster_template_changed)
# - keying by the evolving text would orphan the buffer on every mutation
# and the sample count would rarely reach threshold. cluster_id is stable
# for the lifetime of this process (though not across restarts, same as
# the Drain3 miner state itself).
_cluster_state: dict[tuple[str, int], dict] = {}


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
    # In-memory only; template persistence to Postgres happens explicitly
    # below once a signature's samples clear the LLM + validation gate.
    return TemplateMiner(config=miner_config)


s3 = get_s3_client()
template_miner = build_template_miner()


def fetch_lines(object_key: str) -> tuple[list[str], str | None]:
    obj = s3.get_object(Bucket=config.MINIO_BUCKET, Key=object_key)
    body = gzip.decompress(obj["Body"].read())
    records = [json.loads(line) for line in body.decode("utf-8").splitlines() if line]
    records.sort(key=lambda r: r["line_no"])
    received_at = records[0]["received_at"] if records else None
    return [r["line"] for r in records], received_at


def make_signature(source: str, template_mined: str) -> str:
    return hashlib.sha256(f"{source}::{template_mined}".encode("utf-8")).hexdigest()


def validate_regex(regex_str: str, samples: list[str]) -> tuple[str, int]:
    """Returns (status, matched_count). status is 'active' or 'review'."""
    try:
        compiled = re.compile(regex_str)
    except re.error:
        logger.warning("generated regex failed to compile: %r", regex_str)
        return "review", 0

    matched = sum(1 for s in samples if compiled.search(s))
    if matched / len(samples) >= config.TEMPLATE_VALIDATION_THRESHOLD:
        return "active", matched
    return "review", matched


def generate_and_store_template(signature: str, samples: list[str]) -> dict:
    try:
        regex_str, fields_schema = llm.generate_template(samples)
        model_used = config.OPENROUTER_MODEL
    except Exception:
        logger.exception("LLM template generation failed for signature=%s", signature)
        regex_str, fields_schema, model_used = NEVER_MATCHES_REGEX, {}, "generation-failed"

    status, matched = validate_regex(regex_str, samples)
    logger.info(
        "signature=%s status=%s matched=%d/%d regex=%r",
        signature,
        status,
        matched,
        len(samples),
        regex_str,
    )

    template_id = templates_store.create_template(
        signature, regex_str, fields_schema, samples, model_used, status
    )
    return {"id": template_id, "regex": regex_str, "status": status, "fields_schema": fields_schema}


def process_entry(entry: str, source: str, ts: str | None, channel) -> None:
    result = template_miner.add_log_message(entry)
    cluster_id = result.get("cluster_id")

    logger.info(
        "source=%s change=%s cluster_id=%s cluster_size=%s template=%r",
        source,
        result.get("change_type"),
        cluster_id,
        result.get("cluster_size"),
        result.get("template_mined"),
    )

    key = (source, cluster_id)
    state = _cluster_state.get(key)

    if state is None or state["status"] == "buffering":
        state = state or {"status": "buffering", "buffer": []}
        state["buffer"].append(entry)
        _cluster_state[key] = state
        if len(state["buffer"]) < config.TEMPLATE_SAMPLE_THRESHOLD:
            return

        # By now the cluster's template has usually stabilized (it takes
        # a handful of examples for Drain3 to stop generalizing further),
        # so the mined text is a reasonable basis for the durable signature.
        signature = make_signature(source, result.get("template_mined"))
        existing = templates_store.get_template(signature)
        if existing:
            template_row = existing
        else:
            template_row = generate_and_store_template(signature, state["buffer"])
        state.clear()
        state.update(status=template_row["status"], id=template_row["id"], regex=template_row["regex"])

    if state["status"] != "active":
        return

    match = re.search(state["regex"], entry)
    if not match:
        logger.warning("active template did not match entry, skipping: %r", entry[:200])
        return

    mq.publish_json(
        channel,
        config.REFINED_QUEUE,
        {
            "source": source,
            "template_id": state["id"],
            "ts": ts,
            "raw_message": entry,
            "fields": match.groupdict(),
        },
    )


def handle_message(payload: dict, channel) -> None:
    object_key = payload["object_key"]
    source = payload["source"]

    raw_lines, received_at = fetch_lines(object_key)
    entries = join_multiline(raw_lines)

    for entry in entries:
        process_entry(entry, source, received_at, channel)

    logger.info(
        "processed %s: %d raw lines -> %d logical entries, %d total clusters so far",
        object_key,
        len(raw_lines),
        len(entries),
        len(template_miner.drain.clusters),
    )


if __name__ == "__main__":
    mq.consume_forever(config.TRUSTED_QUEUE, handle_message)
