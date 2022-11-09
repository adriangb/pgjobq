from uuid import UUID


class JobCancelledError(Exception):
    def __init__(self, job: UUID) -> None:
        self.job = job
        super().__init__(f'The job "{job}" was canceled while it was being processed')


class JobDoesNotExist(Exception):
    def __init__(self, job: UUID) -> None:
        self.job = job
        super().__init__(f'The job "{job}" does not exist')


class QueueDoesNotExist(Exception):
    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name
        super().__init__(f'The queue "{queue_name}" does not exist')


class ReceiptHandleExpired(Exception):
    def __init__(self, receipt_handle: int) -> None:
        self.receipt_handle = receipt_handle
        msg = (
            f'The receipt handle "{receipt_handle}" expired.'
            " This likely means this client is failing to send back heartbeats for in-flight jobs"
        )
        super().__init__(msg)
