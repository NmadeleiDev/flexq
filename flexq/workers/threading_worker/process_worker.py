import logging
from multiprocessing import Process, Manager

from flexq.job import JobComposite, JobStatusEnum, Pipeline, Group

from flexq.workers.worker_base import WorkerBase


class ProcessWorker(WorkerBase):
    def _before_start_routine(self):
        # super()._before_start_routine() тут вызов перенесен в _todo_callback, чтобы подключение существовало в каждом процессе
        manager = Manager()

        self._lock = manager.Lock()

        self.running_jobs = manager.list()

    def _acquire_lock(self):
        self._lock.acquire()

    def _release_lock(self):
        self._lock.release()

    def _todo_callback(self, job_name: str, job_id: str):
        logging.debug(f'got job_name={job_name}, job_id={job_id} in _todo_callback')
        if job_id in self.running_jobs:
            logging.warn(f'job {job_name} with id={job_id} passed to _todo_callback, but it is already in self.running_jobs (ignore if it is composite job, they are not acknowledged)')
            return

        super()._before_start_routine()

        if job_name not in self.executors.keys():
            if job_name not in (Pipeline.queue_name, Group.queue_name, JobComposite.queue_name):
                logging.error(f'Job executor "{job_name}" is not known here')
                return
        if self.max_parallel_executors is not None and len(self.running_jobs) >= self.max_parallel_executors:
            logging.info(f'skipping job job_name={job_name}, job_id={job_id} due to max amount of parallel executors running')
            return

        job_process = Process(target=self._try_start_job, args=(job_name, job_id))
        job_process.start()