import logging
from threading import Lock, Thread
from typing import Callable, Union

from flexq.job import JobComposite, JobStatusEnum, Pipeline, Group

from flexq.workers.worker_base import WorkerBase


class ThreadingWorker(WorkerBase):
    def _before_start_routine(self):
        super()._before_start_routine()
        self._lock = Lock()

    def _acquire_lock(self):
        self._lock.acquire()

    def _release_lock(self):
        self._lock.release()

    def _todo_callback(self, job_name: str, job_id: str):
        logging.debug(f'got job_name={job_name}, job_id={job_id} in _todo_callback')
        if job_id in self.running_jobs:
            logging.warn(f'job {job_name} with id={job_id} passed to _todo_callback, but it is already in self.running_jobs (ignore if it is composite job, they are not acknowledged)')
            return

        if job_name not in self.executors.keys():
            if job_name not in (Pipeline.queue_name, Group.queue_name, JobComposite.queue_name):
                logging.error(f'Job executor "{job_name}" is not known here')
                return
        if self.max_parallel_executors is not None and len(self.running_jobs) >= self.max_parallel_executors:
            logging.info(f'skipping job job_name={job_name}, job_id={job_id} due to max amount of parallel executors running')
            return

        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id), daemon=True)
        job_thread.start()
