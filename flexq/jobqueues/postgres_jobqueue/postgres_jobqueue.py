import logging
import select
from typing import Callable, List, Union
import psycopg2
from psycopg2.extensions import Notify
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum


parts_join_char = "__"

class PostgresJobQueue(JobQueueBase):
    def __init__(self, dsn: str) -> None:
        super().__init__()
        self.dsn = dsn

    def subscribe_to_queues(self, queues_names: List[str], todo_callback: Union[Callable, None]=None):
        self.todo_callback = todo_callback

        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                for queue_name in queues_names:
                    for notification_type in NotificationTypeEnum:
                        channel_name = f'{queue_name}{parts_join_char}{notification_type}'.lower()
                        curs.execute(f"LISTEN {channel_name};")
                        logging.debug(f'Listening for channel {channel_name}')

                while True:
                    # read_c, _, exc_c = select.select([conn],[],[])
                    # logging.debug('select on conn triggered')
                    # if len(read_c) > 0:
                    #     conn.poll()
                    #     while conn.notifies:
                    #         notify = conn.notifies.pop(0)
                    #         logging.debug(f"Got NOTIFY: {notify.pid}, {notify.channel}, {notify.payload}")
                    #         self._handle_notification(notify)
                    if select.select([conn],[],[],5) == ([],[],[]):
                        # conn.poll()
                        logging.debug(f'select read loop, notifies: {conn.notifies}')
                        # pass
                    else:
                        conn.poll()
                        while conn.notifies:
                            notify = conn.notifies.pop(0)
                            logging.debug(f"Got NOTIFY: {notify.pid}, {notify.channel}, {notify.payload}")
                            self._handle_notification(notify)

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                channel_name = f'{queue_name}{parts_join_char}{notifycation_type}'.lower()
                curs.execute(f'NOTIFY {channel_name}, %s;', (str(payload), ))
                logging.debug(f'sent notify to channel {channel_name} with payload: {payload}')

    def _handle_notification(self, notification: Notify):
        name_parts = str(notification.channel).split(parts_join_char)
        if len(name_parts) != 2:
            logging.warn(f'Got notification, but its channel name has unexpected format: {notification.channel}, ignoring it')
            return

        job_name = name_parts[0]
        notification_type = name_parts[1]

        if notification_type == NotificationTypeEnum.todo:
            self.todo_callback(job_name, notification.payload)
        else:
            logging.warn(f'Got notification, but its type is not recognized: {notification_type}, ignoring it')
            