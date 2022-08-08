import logging
import select
from typing import Callable, List, Union
import psycopg2
import psycopg2.extensions
from flexq.job import Group, JobComposite, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobqueues.notification import NotificationTypeEnum


from psycopg2.extensions import Notify

from flexq.jobqueues.notification import Notification



class PostgresJobQueue(JobQueueBase):
    def __init__(self, dsn: str) -> None:
        super().__init__()
        self.dsn = dsn

    def _wait_in_queues(self, queues_names: List[str]):
        conn = psycopg2.connect(self.dsn)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        curs = conn.cursor()
        for queue_name in queues_names:
            channel_name = f'{queue_name}{self.parts_join_char}{NotificationTypeEnum.todo}'
            curs.execute(f'LISTEN "{channel_name}";')

            channel_name = str(NotificationTypeEnum.abort)
            curs.execute(f'LISTEN "{channel_name}";')

        curs.execute(f'LISTEN "{JobComposite.queue_name}";')
        curs.execute(f'LISTEN "{Group.queue_name}";')
        curs.execute(f'LISTEN "{Pipeline.queue_name}";')
        
        while True:
            if select.select([conn],[],[],10) != ([],[],[]):
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    self._handle_notification(self.parse_notification(notify))

    def parse_notification(self, notification: Notify) -> Notification:
        if notification.channel == NotificationTypeEnum.abort:
            self.abort_callback(notification.payload)
            return

        name_parts = str(notification.channel).split(self.parts_join_char)
        if len(name_parts) != 2:
            logging.warn(f'Got notification, but its channel name has unexpected format: {notification.channel}, ignoring it')
            return

        job_name = name_parts[0]
        notification_type = name_parts[1]

        return Notification(notification_type, job_name, notification.payload)

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                channel_name = f'{queue_name}{self.parts_join_char}{notifycation_type}'
                curs.execute(f'NOTIFY "{channel_name}", %s;', (str(payload), ))
                logging.debug(f'sent notify to channel {channel_name} with payload: {payload}')

            