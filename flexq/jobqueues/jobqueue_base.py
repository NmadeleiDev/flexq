from enum import Enum
from typing import Callable, List
from flexq.job import Job

class NotificationTypeEnum(str, Enum):
    todo = 'todo'
    done = 'done'

class JobQueueBase:
    def __init__(self, todo_callback: Callable, done_callback: Callable) -> None:
        self.todo_callback = todo_callback
        self.done_callback = done_callback

    def subscribe_to_queues(self, queues_names: List[str], todo_callback: Callable, done_callback: Callable):
        raise NotImplemented

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        raise NotImplemented
