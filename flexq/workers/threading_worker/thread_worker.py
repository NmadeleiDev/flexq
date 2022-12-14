import logging
from threading import Lock, Thread

from flexq.job import Group, JobComposite, JobStatusEnum, Pipeline
from flexq.workers.worker_base import WorkerBase


class ThreadWorker(WorkerBase):
    def _before_start_routine(self):
        super()._before_start_routine()
        self._lock = Lock()

    def _acquire_lock(self):
        self._lock.acquire()

    def _release_lock(self):
        self._lock.release()

    def _call_try_start_job(self, job_name: str, job_id: str):
        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id))
        job_thread.start()
