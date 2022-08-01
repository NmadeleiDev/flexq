import logging
from queue import Queue
import select
from threading import Thread
from typing import Callable

import psycopg2
from flexq.job import JobStatusEnum
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum

from flexq.jobstores.jobstore_base import JobStoreBase
from flexq.workers.worker_base import WorkerBase


class ThreadingWorker(WorkerBase):
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase) -> None:
        self.executors = {}
        self.running_jobs = {}
        self.jobstore = jobstore
        self.jobqueue = jobqueue

    def _todo_callback(self, job_name: str, job_id: str):
        job_thread = Thread(target=self._try_start_job, args=(job_name, job_id))

        job_thread.start()

        self.running_jobs[job_id] = job_thread

    def _done_callback(self, job_name: str, job_id: str):
        job_thread = Thread(target=self._handle_job_finished, args=(job_name, job_id))

        job_thread.start()

        self.running_jobs[job_id] = job_thread

    def _try_start_job(self, job_name: str, job_id: str):
        # пытаемся добавить работу в poll - есть успешно добавлась (т.е. ее там еще не было) , начинаем выполнять
        if self.jobstore.try_acknowledge_job(job_id):
            self.jobstore.set_status_for_job(job_id, JobStatusEnum.acknowledged.value)

            job = self.jobstore.get_job(job_id)
            logging.debug(f'starting job {job_id}')
            self.executors[job_name](*job.args, **job.kwargs)
            logging.debug(f'finished job {job_id}')

            self.jobstore.set_status_for_job(job_id, JobStatusEnum.finished.value)

            queue_names_waiting_for_this = self.jobstore.get_queue_names_interested_in_job(job_id)

            for queue_name in queue_names_waiting_for_this:
                self.jobqueue.send_notify_to_queue(
                    queue_name=queue_name, 
                    notifycation_type=NotificationTypeEnum.done.value, 
                    payload=job_id)
        else:
            logging.debug(f'seems like job {job_id} is already handled by other worker')

    def _handle_job_finished(self, job_name: str, job_id: str):
        
        # проверяем, ожидает ли какая то работа завершения этой.
        # Если да - берем эту работу и _start_job

        jobs_waiting_for_this = self.jobstore.get_jobs_ids_in_queues_waiting_for_job(job_id, list(self.executors.keys()))
        for job_id in jobs_waiting_for_this:
            self._try_start_job(job_name, job_id)