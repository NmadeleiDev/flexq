from typing import Callable
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobstores.jobstore_base import JobStoreBase


class WorkerBase:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase) -> None:
        self.executors = {}
        self.running_jobs = {}
        self.jobstore = jobstore
        self.jobqueue = jobqueue

    def add_job_executor(self, name: str, cb: Callable):
        self.executors[name] = cb

    def wait_for_work(self):
        self.jobqueue.subscribe_to_queues(list(self.executors.keys()), self._todo_callback, self._done_callback)

    def _todo_callback(self, job_name: str, job_id: str):
        pass

    def _done_callback(self, job_name: str, job_id: str):
        pass