import logging
from threading import Thread
from typing import Callable
from flexq.exceptions.worker import RunningJobDuplicate, UnknownJobExecutor

from flexq.job import JobStatusEnum
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum

from flexq.jobstores.jobstore_base import JobStoreBase
from flexq.workers.worker_base import WorkerBase


class ThreadingWorker(WorkerBase):
    def _todo_callback(self, job_name: str, job_id: str):
        if job_id in self.running_jobs.keys():
            raise RunningJobDuplicate(f'job {job_name} with id={job_id} passed to _todo_callback, but its already in self.running_jobs')

        if job_name not in self.executors.keys():
            raise UnknownJobExecutor(f'Job executor "{job_name}" is not known here')

        if self.max_parallel_executors is not None and len(self.running_jobs.keys()) >= self.max_parallel_executors:
            return

        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id))

        job_thread.start()

        self.running_jobs[job_id] = job_thread

    def _try_start_job(self, job_name: str, job_id: str):
        # пытаемся добавить работу в poll - есть успешно добавлась (т.е. ее там еще не было) , начинаем выполнять
        if self.jobstore.try_acknowledge_job(job_id):
            self.jobstore.set_status_for_job(job_id, JobStatusEnum.acknowledged.value)

            job = self.jobstore.get_job(job_id)
            logging.debug(f'starting job {job_id}')
            job.result = self.executors[job_name](*job.args, **job.kwargs)
            logging.debug(f'finished job {job_id}')

            self.jobstore.set_status_for_job(job_id, JobStatusEnum.finished.value)

            if self.store_results and job.result is not None:
                self.jobstore.save_result_for_job(job_id, job.get_result_bytes())

            for waiting_job_id, waiting_queue_name in self.jobstore.get_waiting_for_job(job_id):
                self.jobqueue.send_notify_to_queue(
                    queue_name=waiting_queue_name, 
                    notifycation_type=NotificationTypeEnum.todo.value, 
                    payload=waiting_job_id)
        else:
            logging.debug(f'seems like job {job_id} is already handled by other worker')

    def inspect_running_jobs(self):
        for job_id, job_thread in self.running_jobs.items():
            if not job_thread.is_alive():
                del self.running_jobs[job_id]

        if self.max_parallel_executors is not None and len(self.running_jobs.keys()) < self.max_parallel_executors: 
            for waiting_job_id, waiting_queue_name in self.jobstore.get_not_acknowledged_jobs_ids_in_queues(self.executors.keys()):
                self._todo_callback(waiting_queue_name, waiting_job_id)
