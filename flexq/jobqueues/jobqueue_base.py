from enum import Enum
from typing import Callable, List, Union
from flexq.job import Job

class NotificationTypeEnum(str, Enum):
    todo = 'todo'
    done = 'done'

class JobQueueBase:
    def __init__(self) -> None:
        self.todo_callback = None

    def subscribe_to_queues(self, queues_names: List[str], todo_callback: Union[Callable, None]=None):
        raise NotImplemented

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        raise NotImplemented
