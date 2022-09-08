import logging
from abc import ABC
from typing import Callable, List, Union
from flexq.jobqueues.notification import Notification, NotificationTypeEnum


class JobQueueBase(ABC):
    parts_join_char = "__"

    def __init__(self) -> None:
        """
        It's important that JobStore object do not create anything unpickable in __init__. Creation of all connections must be in self.init_conn
        """
        self.todo_callback = None

    def init_conn(self):
        pass

    def subscribe_to_queues(self,
                            queues_names: List[str],
                            todo_callback: Union[Callable, None] = None,
                            abort_callback: Union[Callable, None] = None):
        self.todo_callback = todo_callback
        self.abort_callback = abort_callback
        self._wait_in_queues(queues_names)

    def _wait_in_queues(self, queues_names: List[str]):
        pass

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        pass

    def _handle_notification(self, notification: Notification):
        logging.debug(f'got notification: {str(notification)}')

        if notification.notification_type == NotificationTypeEnum.todo:
            self.todo_callback(notification.job_name, notification.job_id)
        else:
            logging.warn(
                f'Got notification for job "{notification.job_name}", but its type is not recognized: {notification.notification_type}, ignoring it')
