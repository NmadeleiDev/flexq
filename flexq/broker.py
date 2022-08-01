import logging
from typing import Dict, List
from flexq.exceptions.broker import FailedToEnqueueJob

from flexq.job import Job
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase


class Broker:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase) -> None:
        self.jobstore = jobstore
        self.jobqueue = jobqueue

    def _save_job_to_jobstore(self, job: Job):
        if self.jobstore.add_job_to_queue(job):
            self.jobqueue.send_notify_to_queue(queue_name=job.queue_name, notifycation_type=NotificationTypeEnum.todo.value, payload=job.id)
        else:
            raise FailedToEnqueueJob(f'can not put job into queue, job: {job}')

    def add_job(self, queue_name: str, args: List, kwargs: Dict[str, any]) -> Job:
        job = Job(queue_name=queue_name, args=args, kwargs=kwargs)
        self._save_job_to_jobstore(job)

