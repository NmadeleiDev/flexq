import logging
from typing import Dict, List, Tuple, Union
from flexq.exceptions.broker import FailedToEnqueueJob, UnknownSchedulingMethod
from flexq.exceptions.jobstore import JobNotFoundInStore

from flexq.job import Group, Job, JobStatusEnum, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobqueues.notification import NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


class Broker:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase, run_inspection_every_n_minutes=5) -> None:
        self.jobstore = jobstore
        self.jobqueue = jobqueue

        self.run_inspection_every_n_minutes = run_inspection_every_n_minutes

        self.start_running_jobs_inspector()

    def add_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        self.launch_job(self.register_job(job))
        return job

    def register_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        self._add_scheduler_job_if_schedule_present(job)

        if self.jobstore.add_job_to_store(job):
            return job
        else:
            raise FailedToEnqueueJob(f'can not put job into queue, job: {job}')

    def launch_job(self, job: Union[Job, Group, Pipeline]):
        self.jobqueue.send_notify_to_queue(queue_name=job.queue_name, notifycation_type=NotificationTypeEnum.todo.value, payload=job.id)

    def remove_job(self, job_id: str):
        self.jobstore.remove_job_from_store(job_id)
        self._remove_scheduler_job_if_present(job_id)

    def start_running_jobs_inspector(self):
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }

        self.scheduler = BackgroundScheduler(job_defaults=job_defaults)
        self.scheduler.start()

        self.inspection_job = self.scheduler.add_job(self.inspect_running_jobs, 'interval', minutes=self.run_inspection_every_n_minutes)

    def inspect_running_jobs(self):
        for to_launch_job_id, to_launch_queue_name in self.jobstore.get_not_acknowledged_jobs_ids_and_queue_names():
            logging.debug(f'calling send_notify_to_queue for job name "{to_launch_queue_name}", id={to_launch_job_id}')
            self.jobqueue.send_notify_to_queue(queue_name=to_launch_queue_name, notifycation_type=NotificationTypeEnum.todo.value, payload=to_launch_job_id)

    def try_relaunch_job(self, job_id: str, do_send_launch=True):
        # получить job
        # посчитать кол-во не завершенных реплик (по parent_id)
        # если кол-во незавершенных ок - поставить status=created
        # отослать launch job
        
        try:
            job = self.jobstore.get_job(job_id)
        except JobNotFoundInStore:
            logging.debug(f'job_id={job_id} not present in store, skipping scheduled task')
            return

        children = self.jobstore.get_child_job_ids(job_id)
        for child_job_id in children:
            self.try_relaunch_job(child_job_id, do_send_launch=False)

        if job.status == JobStatusEnum.acknowledged.value:
            logging.debug(f'unable to launch job name={job.queue_name}, id={job.id} since latest execution is not finished yet')
            return

        job.status == JobStatusEnum.created.value

        self.jobstore.update_job_in_store(job)

        if do_send_launch:
            self.launch_job(job)

    def _add_scheduler_job_if_schedule_present(self, job: Job):
        scheduler_job_id = f'scheduled_job_{job.id}'
        if job.cron is not None:
            minute, hour, month_day, month, week_day = job.cron.split(' ')
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=month_day,
                day_of_week=week_day,
                month=month,
            )
        elif job.interval_name is not None:
            kwargs = {job.interval_name: job.interval_value}
            trigger = IntervalTrigger(
                **kwargs
            )
        else:
            return

        present_jobs = [x.id for x in self.scheduler.get_jobs()]
        if scheduler_job_id not in present_jobs:
            self.scheduler.add_job(self.try_relaunch_job, args=(job.id, ), trigger=trigger, id=scheduler_job_id, coalesce=True)
            logging.debug(f'added scheduled_job for job {job} as id {scheduler_job_id}')

    def _remove_scheduler_job_if_present(self, job_id: str):
        scheduler_job_id = f'scheduled_job_{job_id}'

        present_jobs = [x.id for x in self.scheduler.get_jobs()]
        if scheduler_job_id in present_jobs:
            self.scheduler.remove_job(scheduler_job_id)
            logging.debug(f'removed scheduled_job for job_id={job_id} with scheduler id = {scheduler_job_id}')
    



