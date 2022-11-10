import logging
import select
from typing import Callable, List, Union
import psycopg2
import psycopg2.extensions
from flexq.job import Group, JobComposite, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobqueues.notification import NotificationTypeEnum
from flexq.exceptions.jobqueue import JobQueueException

from psycopg2.extensions import Notify

from flexq.jobqueues.notification import Notification


class PostgresJobQueue(JobQueueBase):
    def __init__(self, dsn: str, instance_name='default') -> None:
        super().__init__(instance_name=instance_name)
        self.dsn = dsn

    def _wait_in_queues(self, queues_names: List[str]):
        conn = psycopg2.connect(self.dsn)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        curs = conn.cursor()
        channel_names_to_listen = []
        for queue_name in queues_names + [JobComposite.queue_name, Group.queue_name, Pipeline.queue_name]:
            channel_names_to_listen.append(self.parts_join_char.join([self.instance_name, queue_name, NotificationTypeEnum.todo]))
            if queue_name in queues_names:
                channel_names_to_listen.append(self.parts_join_char.join([self.instance_name, queue_name, NotificationTypeEnum.abort]))

        for channel_name in channel_names_to_listen:
            curs.execute(f'LISTEN "{channel_name}";')

        logging.debug(f'subscribed to channels: {", ".join(channel_names_to_listen)}')

        while True:
            if select.select([conn], [], [], 10) != ([], [], []):
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    self._handle_notification(self.parse_notification(notify))

    def parse_notification(self, notification: Notify) -> Notification:
        name_parts = str(notification.channel).split(self.parts_join_char)
        if len(name_parts) != 3:
            raise JobQueueException(
                f'Got notification, but its channel name has unexpected format: {notification.channel}, ignoring it')

        instance_name = name_parts[0]
        if instance_name != self.instance_name:
            raise JobQueueException(f'strange, got notification from wrong instance {instance_name}: {notification.channel}')

        job_name = name_parts[1]
        notification_type = name_parts[2]

        return Notification(notification_type, job_name, notification.payload)

    def send_notify_to_queue(self, queue_name: str, notification_type: NotificationTypeEnum, payload: str):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                channel_name = self.parts_join_char.join([self.instance_name, queue_name, notification_type])

                curs.execute(f'NOTIFY "{channel_name}", %s;', (str(payload),))
                logging.debug(f'sent notify to channel {channel_name} with payload: {payload}')
