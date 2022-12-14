import logging
from multiprocessing import Manager, Process

from flexq.job import Group, JobComposite, JobStatusEnum, Pipeline
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

    def _call_try_start_job(self, job_name: str, job_id: str):
        job_process = Process(target=self._try_start_job, args=(job_name, job_id))
        job_process.start()
