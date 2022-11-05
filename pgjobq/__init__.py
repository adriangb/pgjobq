from pgjobq._crud import create_queue, delete_queue
from pgjobq._exceptions import JobDoesNotExist, QueueDoesNotExist, ReceiptHandleExpired
from pgjobq._filters import Attribute, JobIdIn
from pgjobq._migrations import migrate_to_latest_version
from pgjobq._queue import connect_to_queue
from pgjobq.api import OutgoingJob, Queue


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
    "OutgoingJob",
    "Attribute",
    "JobIdIn",
    "JobDoesNotExist",
    "QueueDoesNotExist",
    "ReceiptHandleExpired",
)
