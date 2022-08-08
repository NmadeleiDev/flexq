import logging
from threading import Lock, Thread
from typing import Callable, Union
from flexq.exceptions.worker import RunningJobDuplicate, UnknownJobExecutor

from flexq.job import JobStatusEnum, Pipeline, Group
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobstores.jobstore_base import JobStoreBase

from flexq.workers.worker_base import WorkerBase


class ThreadingWorker(WorkerBase):
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase, max_parallel_executors: Union[int, None] = None, store_results=True, run_inspection_every_n_minutes=2) -> None:
        super().__init__(jobstore, jobqueue, max_parallel_executors, store_results, run_inspection_every_n_minutes)

        self._lock = Lock()

    def _acquire_lock(self):
        self._lock.acquire()

    def _release_lock(self):
        self._lock.release()

    def _todo_callback(self, job_name: str, job_id: str):
        logging.debug(f'got job_name={job_name}, job_id={job_id} in _todo_callback')
        if job_id in self.running_jobs:
            raise RunningJobDuplicate(f'job {job_name} with id={job_id} passed to _todo_callback, but it is already in self.running_jobs')

        if job_name not in self.executors.keys():
            if job_name not in (Pipeline.queue_name, Group.queue_name):
                raise UnknownJobExecutor(f'Job executor "{job_name}" is not known here')
        else:
            if self.max_parallel_executors is not None and len(self.running_jobs) >= self.max_parallel_executors:
                logging.debug(f'skipping job job_name={job_name}, job_id={job_id} due to max amount of parallel executors running')
                return

        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id))
        job_thread.start()

    # def _abort_callback(self, job_id: str):
    #     if job_id in self.running_jobs.keys():
    #         logging.debug(f'aborting job_id={job_id}')
    #         job_thread = self.running_jobs[job_id]
    #         if 

