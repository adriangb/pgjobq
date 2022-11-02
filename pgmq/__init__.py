from pgmq._crud import create_queue, delete_queue
from pgmq._filters import Attribute
from pgmq._migrations import migrate_to_latest_version
from pgmq._queue import connect_to_queue
from pgmq.api import OutgoingMessage, Queue


def get_dlq_name(queue_name: str) -> str:
    return f"dlq@{queue_name}"


__all__ = (
    "connect_to_queue",
    "create_queue",
    "delete_queue",
    "delete_queue",
    "get_dlq_name",
    "migrate_to_latest_version",
    "Queue",
    "OutgoingMessage",
    "Attribute",
)
