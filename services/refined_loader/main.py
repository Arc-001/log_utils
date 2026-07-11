import logging

from psycopg2.extras import Json

from common import config, db, mq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_log(source: str, template_id: int, ts: str | None, raw_message: str, fields: dict) -> None:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO logs (source, template_id, ts, raw_message, fields)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (source, template_id, ts, raw_message, Json(fields)),
            )
        conn.commit()
    finally:
        conn.close()


def handle_message(payload: dict, channel) -> None:
    insert_log(
        payload["source"],
        payload["template_id"],
        payload.get("ts"),
        payload["raw_message"],
        payload["fields"],
    )
    logger.info("inserted log source=%s template_id=%s", payload["source"], payload["template_id"])


if __name__ == "__main__":
    mq.consume_forever(config.REFINED_QUEUE, handle_message)
