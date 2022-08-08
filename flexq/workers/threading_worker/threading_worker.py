import logging
from threading import Thread
from typing import Callable
from flexq.exceptions.worker import RunningJobDuplicate, UnknownJobExecutor

from flexq.job import JobStatusEnum, Pipeline, Group
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum

from flexq.jobstores.jobstore_base import JobStoreBase
from flexq.workers.worker_base import WorkerBase


class ThreadingWorker(WorkerBase):
    def _todo_callback(self, job_name: str, job_id: str):
        logging.debug(f'got job_name={job_name}, job_id={job_id} in _todo_callback')
        if job_id in self.running_jobs.keys():
            raise RunningJobDuplicate(f'job {job_name} with id={job_id} passed to _todo_callback, but it is already in self.running_jobs')

        if job_name not in self.executors.keys():
            if job_name not in (Pipeline.queue_name, Group.queue_name):
                raise UnknownJobExecutor(f'Job executor "{job_name}" is not known here')
        else:
            if self.max_parallel_executors is not None and len(self.running_jobs.keys()) >= self.max_parallel_executors:
                return

        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id))

        job_thread.start()

        self.running_jobs[job_id] = job_thread

    # def _abort_callback(self, job_id: str):
    #     if job_id in self.running_jobs.keys():
    #         logging.debug(f'aborting job_id={job_id}')
    #         job_thread = self.running_jobs[job_id]
    #         if 

    def inspect_running_jobs(self):
        running_jobs = list(self.running_jobs.items())
        for job_id, job_thread in running_jobs:
            if not job_thread.is_alive():
                del self.running_jobs[job_id]

        generic_queue_names = [Group.queue_name, Pipeline.queue_name]

        if self.max_parallel_executors is not None and len(self.running_jobs.keys()) < self.max_parallel_executors: 
            for waiting_job_id, waiting_queue_name in self.jobstore.get_not_acknowledged_jobs_ids_in_queues(list(self.executors.keys()) + generic_queue_names):
                logging.debug(f'calling _todo_callback for job name "{waiting_queue_name}", id={waiting_job_id}')
                self._todo_callback(waiting_queue_name, waiting_job_id)
