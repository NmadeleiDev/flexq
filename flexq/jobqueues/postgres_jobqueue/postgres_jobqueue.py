import logging
import select
from typing import Callable, List, Union
import psycopg2
import psycopg2.extensions
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum



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

        while True:
            read_c, _, exc_c = select.select([conn],[],[])
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                self._handle_notification(notify)

    def send_notify_to_queue(self, queue_name: str, notifycation_type: NotificationTypeEnum, payload: str):
        with psycopg2.connect(self.dsn) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as curs:
                channel_name = f'{queue_name}{self.parts_join_char}{notifycation_type}'
                curs.execute(f'NOTIFY "{channel_name}", %s;', (str(payload), ))
                logging.debug(f'sent notify to channel {channel_name} with payload: {payload}')

            