import logging

from psycopg2.extras import Json

from common import config, db, mq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_log(
    source: str, template_id: int, ts: str | None, raw_message: str, fields: dict, line_hash: str | None
) -> bool:
    """Returns True if a new row was inserted, False if it was a dedup no-op."""
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO logs (source, template_id, ts, raw_message, fields, line_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (line_hash) DO NOTHING
                RETURNING id
                """,
                (source, template_id, ts, raw_message, Json(fields), line_hash),
            )
            inserted = cur.fetchone() is not None
        conn.commit()
        return inserted
    finally:
        conn.close()


def handle_message(payload: dict, channel) -> None:
    inserted = insert_log(
        payload["source"],
        payload["template_id"],
        payload.get("ts"),
        payload["raw_message"],
        payload["fields"],
        payload.get("line_hash"),
    )
    if inserted:
        logger.info("inserted log source=%s template_id=%s", payload["source"], payload["template_id"])
    else:
        logger.info(
            "skipped duplicate (replay) log source=%s template_id=%s",
            payload["source"],
            payload["template_id"],
        )


if __name__ == "__main__":
    mq.consume_forever(config.REFINED_QUEUE, handle_message)
