import psycopg2

from . import config


def get_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )
