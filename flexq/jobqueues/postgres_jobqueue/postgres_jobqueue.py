import logging
import select
from typing import Callable, List
import psycopg2
from psycopg2.extensions import Notify
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum


class PostgresJobQueue(JobQueueBase):
    def __init__(self, dsn: str, todo_callback: Callable, done_callback: Callable) -> None:
        super().__init__(todo_callback, done_callback)
        self.dsn = dsn

    def subscribe_to_queues(self, queues_names: List[str]):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                for queue_name in queues_names:
                    for notification_type in NotificationTypeEnum:
                        curs.execute("LISTEN $1", (f'{queue_name}|{notification_type}',))

                while True:
                    read_c, _, exc_c = select.select([conn],[],[])
                    if len(read_c) > 0:
                        conn.poll()
                        while conn.notifies:
                            notify = conn.notifies.pop(0)
                            logging.debug(f"Got NOTIFY: {notify.pid}, {notify.channel}, {notify.payload}")
                            self._handle_notification(notify)
                    # if select.select([conn],[],[],5) == ([],[],[]):
                    #     print "Timeout"
                    # else:
                    #     conn.poll()
                    #     while conn.notifies:
                    #         notify = conn.notifies.pop(0)
                    #         print "Got NOTIFY:", notify.pid, notify.channel, notify.payload

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                curs.execute('NOTIFY %s, %s', (f'{queue_name}|{notifycation_type}', payload))

    def _handle_notification(self, notification: Notify):
        name_parts = str(notification.channel).split('|')
        if len(name_parts) != 2:
            logging.warn(f'Got notification, but its channel name has unexpected format: {notification.channel}, ignoring it')
            return

        job_name = name_parts[0]
        notification_type = name_parts[1]

        if notification_type == NotificationTypeEnum.todo:
            self.todo_callback(job_name, notification.payload)
        elif notification_type == NotificationTypeEnum.done:
            self.done_callback(job_name, notification.payload)
        else:
            logging.warn(f'Got notification, but its type is not recognized: {notification_type}, ignoring it')
            