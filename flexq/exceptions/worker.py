import logging
from datetime import datetime, timedelta
from typing import Optional, Union


class WorkerException(Exception):
    pass


class JobExecutorExists(WorkerException):
    pass


class UnknownJobExecutor(WorkerException):
    pass


class RunningJobDuplicate(WorkerException):
    pass


class RetryLater(WorkerException):
    def __init__(
        self,
        message,
        next_run_datetime: Optional[datetime] = None,
        delay_seconds: Optional[int] = None,
    ):
        super().__init__(message)

        if next_run_datetime is None and delay_seconds is None:
            delay_seconds = 60
            logging.warning(
                f"both next_run_datetime and delay_seconds are None. Set delay_seconds to 60."
            )

        self.next_run_datetime = next_run_datetime
        self.delay_seconds = delay_seconds

    def get_next_run_time(self):
        if self.next_run_datetime is not None:
            return self.next_run_datetime
        else:
            return datetime.now() + timedelta(seconds=self.delay_seconds)
