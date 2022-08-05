import logging
from typing import Dict, List, Tuple, Union
from flexq.exceptions.broker import FailedToEnqueueJob

from flexq.job import Group, Job, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase


class Broker:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase) -> None:
        self.jobstore = jobstore
        self.jobqueue = jobqueue

    def create_job(self, queue_name: str, args: List[any] = [], kwargs: Dict[str, any] = {}) -> Job:
        job = Job(queue_name=queue_name, args=args, kwargs=kwargs)
        return self.add_job(job)

    def add_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        return self.launch_job(self.register_job(job))

    def register_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        if self.jobstore.add_job_to_store(job):
            return job
        else:
            raise FailedToEnqueueJob(f'can not put job into queue, job: {job}')

    def launch_job(self, job: Union[Job, Group, Pipeline]):
        self.jobqueue.send_notify_to_queue(queue_name=job.queue_name, notifycation_type=NotificationTypeEnum.todo.value, payload=job.id)


