from enum import Enum
import logging
from typing import Callable, List, Union
from flexq.job import Job
from psycopg2.extensions import Notify

class NotificationTypeEnum(str, Enum):
    todo = 'todo'
    done = 'done'
    abort = 'abort'

class JobQueueBase:
    parts_join_char = "__"
    
    def __init__(self) -> None:
        self.todo_callback = None

    def subscribe_to_queues(self, 
            queues_names: List[str], 
            todo_callback: Union[Callable, None]=None,
            abort_callback: Union[Callable, None]=None):
        self.todo_callback = todo_callback
        self.abort_callback = abort_callback
        self._wait_in_queues(queues_names)

    def _wait_in_queues(self, queues_names: List[str]):
        raise NotImplemented    

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        raise NotImplemented

    def _handle_notification(self, notification: Notify):
        if notification.channel == NotificationTypeEnum.abort:
            self.abort_callback(notification.payload)
            return

        name_parts = str(notification.channel).split(self.parts_join_char)
        if len(name_parts) != 2:
            logging.warn(f'Got notification, but its channel name has unexpected format: {notification.channel}, ignoring it')
            return

        job_name = name_parts[0]
        notification_type = name_parts[1]

        if notification_type == NotificationTypeEnum.todo:
            self.todo_callback(job_name, notification.payload)
        else:
            logging.warn(f'Got notification for job "{job_name}", but its type is not recognized: {notification_type}, ignoring it')
