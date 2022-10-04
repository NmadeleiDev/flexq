import logging
from abc import ABC
from typing import Callable, List, Union
from flexq.jobqueues.notification import Notification, NotificationTypeEnum


class JobQueueBase(ABC):
    parts_join_char = "__"

    def __init__(self, instance_name='default') -> None:
        """
        It's important that JobStore object do not create anything unpickable in __init__. Creation of all connections must be in self.init_conn
        """
        self.instance_name = instance_name

        self.done_callback = None
        self.todo_callback = None
        self.abort_callback = None

    def init_conn(self):
        pass

    def subscribe_to_queues(self,
                            queues_names: List[str],
                            todo_callback: Union[Callable, None] = None,
                            done_callback: Union[Callable, None] = None,
                            abort_callback: Union[Callable, None] = None):
        self.todo_callback = todo_callback
        self.done_callback = done_callback
        self.abort_callback = abort_callback
        self._wait_in_queues(queues_names)

    def _wait_in_queues(self, queues_names: List[str]):
        pass

    def send_notify_to_queue(self, queue_name: str, notification_type: NotificationTypeEnum, payload: str):
        pass

    def _handle_notification(self, notification: Notification):
        logging.debug(f'got notification: {str(notification)}')

        if notification.notification_type == NotificationTypeEnum.todo and self.todo_callback is not None:
            self.todo_callback(notification.job_name, notification.job_id)
        elif notification.notification_type == NotificationTypeEnum.done and self.done_callback is not None:
            self.done_callback(notification.job_name, notification.job_id)
        else:
            logging.warning(
                f'Got notification for job "{notification.job_name}", but its type is not recognized: {notification.notification_type}, ignoring it')
