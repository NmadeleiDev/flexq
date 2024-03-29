import logging
from datetime import datetime, timedelta
from typing import Union

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from flexq.exceptions.broker import FailedToEnqueueJob
from flexq.job import Group, Job, JobStatusEnum, Pipeline, composite_job_classes
from flexq.jobqueues.jobqueue_base import JobQueueBase
from flexq.jobqueues.notification import NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase


class Broker:
    def __init__(
        self,
        jobstore: JobStoreBase,
        jobqueue: JobQueueBase,
        run_inspection_every_n_minutes=5,
        is_master=False,
    ) -> None:
        self.jobstore = jobstore
        self.jobqueue = jobqueue

        self.jobstore.init_conn()
        self.jobqueue.init_conn()

        self.run_inspection_every_n_minutes = run_inspection_every_n_minutes

        self.is_master = is_master

        self.start_jobs_inspector()
        if self.is_master:
            self.init_scheduled_jobs()

    def add_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        self.launch_job(self.register_job(job))
        return job

    def register_job(self, job: Union[Job, Group, Pipeline]) -> Job:
        if not self.jobstore.add_job_to_store(job):
            raise FailedToEnqueueJob(f"can not put job into queue, job: {job}")
        self._add_scheduler_job_if_schedule_present(job)
        return job

    def launch_job(self, job: Union[Job, Group, Pipeline]):
        self.jobqueue.send_notify_to_queue(
            queue_name=job.queue_name,
            notification_type=NotificationTypeEnum.todo,
            payload=job.id,
        )

    def remove_job(self, job_id: str):
        self.jobstore.remove_job_from_store(job_id)
        self._remove_scheduler_job_if_present(job_id)

    def start_jobs_inspector(self):
        job_defaults = {"coalesce": True, "max_instances": 1}

        self.scheduler = BackgroundScheduler(job_defaults=job_defaults)
        self.scheduler.start()

        self.inspection_job = self.scheduler.add_job(
            self.inspect_running_jobs,
            trigger="interval",
            minutes=self.run_inspection_every_n_minutes,
        )

    def inspect_running_jobs(self):
        logging.debug(f'current jobs in scheduler: '
                      f'{[(j.id, j.name, j.next_run_time, j.trigger) for j in self.scheduler.get_jobs()]}')
        for (
            to_launch_job_id,
            to_launch_queue_name,
        ) in self.jobstore.get_not_acknowledged_jobs_ids_and_queue_names():
            logging.debug(
                f"Sending notify for job {to_launch_job_id} in queue {to_launch_queue_name} as it is not acknowledged"
            )
            self.jobqueue.send_notify_to_queue(
                queue_name=to_launch_queue_name,
                notification_type=NotificationTypeEnum.todo,
                payload=to_launch_job_id,
            )

        retry_until_success_jobs = self.jobstore.get_jobs(
            retry_until_success_only=True, status=JobStatusEnum.failed
        )
        if retry_until_success_jobs is not None:
            for job in retry_until_success_jobs:
                if (
                    datetime.now() - job.finished_at
                ).total_seconds() / 60 > job.retry_delay_minutes:
                    logging.debug(
                        f"relaunching job {job} since it is in failed state and finished more "
                        f"than {job.retry_delay_minutes} minutes ago (finished_at={job.finished_at})"
                    )
                    self.try_relaunch_job(job.id)

        for job in self.jobstore.get_jobs(status=JobStatusEnum.retry) or []:
            if job.start_timestamp >= datetime.now():
                logging.debug(f'relaunching job {job} because its time for restart')
                self.try_relaunch_job(job.id)

        missed_heartbeat_jobs = self.jobstore.get_jobs(
            heartbeat_missed_by_more_than_n_seconds=60,
            status=JobStatusEnum.acknowledged,
        )  # composite jobs сюда никогда не попадут, т.к. мы их никогда не acknowledge
        if missed_heartbeat_jobs is not None:
            for job in missed_heartbeat_jobs:
                logging.debug(
                    f"seems like job ({job}) is not handled by any worker (last heartbeat {job.last_heartbeat_ts}), "
                    f"(status={job.status}) will retry it"
                )
                self.try_relaunch_job(job.id, relaunch_if_acknowledged=True)

    def init_scheduled_jobs(self):
        scheduled_jobs = self.jobstore.get_jobs(with_schedule_only=True)
        if scheduled_jobs is not None:
            for job in scheduled_jobs:
                self._add_scheduler_job_if_schedule_present(job)
            self.scheduler.wakeup()

    def try_relaunch_job(
        self,
        job_id: str,
        do_send_launch=True,
        relaunch_if_acknowledged=False,
        ignore_start_when_other_job_id_success=False,
    ):
        # получить job
        # посчитать кол-во не завершенных реплик (по parent_id)
        # если кол-во незавершенных ок - поставить status=created
        # отослать launch job

        logging.debug(
            f"Trying to relaunch job_id={job_id}, do_send_launch={do_send_launch}, "
            f"relaunch_if_acknowledged={relaunch_if_acknowledged}"
        )
        jobs_found = self.jobstore.get_jobs(job_id)
        if len(jobs_found) == 0:
            logging.debug(
                f"job_id={job_id} not present in store, skipping scheduled task"
            )
            return
        job = jobs_found[0]

        if (
            not ignore_start_when_other_job_id_success
            and job.start_when_other_job_id_success is not None
        ):
            start_after_job = self.jobstore.get_jobs(
                job.start_when_other_job_id_success
            )[0]
            if start_after_job.status != JobStatusEnum.success:
                logging.debug(
                    f"Skipping relaunch job {job} since it is waiting for job "
                    f"is {job.start_when_other_job_id_success} and its status is {start_after_job.status}"
                )
                return

        children = self.jobstore.get_child_job_ids(job_id)
        for child_job_id in children:
            self.try_relaunch_job(
                child_job_id,
                do_send_launch=False,
                relaunch_if_acknowledged=relaunch_if_acknowledged,
            )

        if job.status == JobStatusEnum.acknowledged and not relaunch_if_acknowledged:
            logging.debug(
                f"unable to launch job name={job.queue_name}, id={job.id} since latest execution is not finished yet"
            )
            return

        job.status = JobStatusEnum.created
        self.jobstore.set_status_for_job(job.id, JobStatusEnum.created,
                                         if_not_acknowledged_only=not relaunch_if_acknowledged)
        logging.debug(f"updated job {job} status to {job.status}")

        if do_send_launch:
            self.launch_job(job)

    def _add_scheduler_job_if_schedule_present(self, job: Job):
        next_run_time = None

        scheduler_job_id = f"scheduled_job_{job.id}"
        if job.cron is not None:
            minute, hour, month_day, month, week_day = job.cron.split(" ")
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=month_day,
                day_of_week=week_day,
                month=month,
            )
        elif job.interval_name is not None:
            kwargs = {job.interval_name: job.interval_value}
            trigger = IntervalTrigger(**kwargs)
            if isinstance(job.finished_at, datetime):
                next_run_time = job.finished_at + timedelta(**kwargs)
            else:
                next_run_time = datetime.now()
        else:
            return

        present_jobs = [x.id for x in self.scheduler.get_jobs()]
        if scheduler_job_id not in present_jobs:
            self.scheduler.add_job(
                self.try_relaunch_job,
                args=(job.id,),
                trigger=trigger,
                id=scheduler_job_id,
                coalesce=True,
                next_run_time=next_run_time,
            )
            logging.debug(
                f"added scheduled_job for job {job}, finished_at={job.finished_at} as id {scheduler_job_id}, "
                f"trigger={trigger}"
            )

    def _remove_scheduler_job_if_present(self, job_id: str):
        scheduler_job_id = f"scheduled_job_{job_id}"

        present_jobs = [x.id for x in self.scheduler.get_jobs()]
        if scheduler_job_id in present_jobs:
            self.scheduler.remove_job(scheduler_job_id)
            logging.debug(
                f"removed scheduled_job for job_id={job_id} with scheduler id = {scheduler_job_id}"
            )
