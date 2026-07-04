import json
import logging
import time
from typing import Callable

import pika

from . import config

logger = logging.getLogger(__name__)


def get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST, credentials=credentials, heartbeat=30
    )
    return pika.BlockingConnection(params)


def publish_json(channel: pika.channel.Channel, routing_key: str, payload: dict) -> None:
    channel.basic_publish(
        exchange=config.EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def consume_forever(
    queue_name: str, handler: Callable[[dict, pika.channel.Channel], None], prefetch: int = 1
) -> None:
    """Connects with retry and consumes queue_name with manual ack.

    handler(payload, channel) should raise on failure; the message is then
    nacked without requeue so it lands on the queue's DLQ. The channel is
    passed through so the handler can publish downstream messages (e.g. the
    next stage's job queue) before the original message is acked.
    """
    while True:
        try:
            connection = get_connection()
            channel = connection.channel()
            channel.basic_qos(prefetch_count=prefetch)

            def _on_message(ch, method, properties, body):
                try:
                    payload = json.loads(body)
                    handler(payload, ch)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception(
                        "handler failed for message on %s, routing to DLQ", queue_name
                    )
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=queue_name, on_message_callback=_on_message)
            logger.info("consuming from %s", queue_name)
            channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            logger.warning("lost connection to rabbitmq, retrying in 5s")
            time.sleep(5)
