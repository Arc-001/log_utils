from psycopg2.extras import Json, RealDictCursor

from common import db


def get_template(signature: str) -> dict | None:
    conn = db.get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, signature, regex, status, fields_schema FROM templates WHERE signature = %s",
                (signature,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def create_template(
    signature: str,
    regex: str,
    fields_schema: dict,
    sample_lines: list[str],
    model_used: str,
    status: str,
) -> int:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO templates (signature, regex, fields_schema, sample_lines, model_used, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (signature) DO UPDATE SET
                    regex = EXCLUDED.regex,
                    fields_schema = EXCLUDED.fields_schema,
                    sample_lines = EXCLUDED.sample_lines,
                    model_used = EXCLUDED.model_used,
                    status = EXCLUDED.status
                RETURNING id
                """,
                (signature, regex, Json(fields_schema), sample_lines, model_used, status),
            )
            template_id = cur.fetchone()[0]
        conn.commit()
        return template_id
    finally:
        conn.close()
