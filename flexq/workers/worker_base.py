import logging
from typing import Callable, Union
from flexq.executor import Executor
from flexq.exceptions.worker import JobExecutorExists
from flexq.job import Group, Job, JobStatusEnum, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase
from apscheduler.schedulers.background import BackgroundScheduler


class WorkerBase:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase, max_parallel_executors:Union[int, None]=None, store_results=True, run_inspetion_every_n_minutes=2) -> None:
        self.executors = {}
        self.running_jobs = {}
        self.jobstore = jobstore
        self.jobqueue = jobqueue

        self.store_results = store_results
        self.max_parallel_executors = max_parallel_executors

        self.run_inspetion_every_n_minutes = run_inspetion_every_n_minutes

        self.start_running_jobs_inspector()

    def add_job_executor(self, name: str, cb: Union[Callable, Executor]):
        if name not in self.executors.keys():
            self.executors[name] = cb
        else:
            raise JobExecutorExists(f'Job executor name "{name}" exists. Add executor with other name.')

    def _try_start_job(self, job_id: str):
        # пытаемся добавить работу в poll - есть успешно добавлась (т.е. ее там еще не было) , начинаем выполнять
        if self.jobstore.try_acknowledge_job(job_id):
            
            job = self.jobstore.get_job(job_id)

            if job.queue_name == Group.internal_queue_name:
                successfull_jobs_count = 0

                for ingroup_job_id in job.args:
                    ingroup_job = self.jobstore.get_job(ingroup_job_id)
                    if ingroup_job.status == JobStatusEnum.created:
                        self.jobqueue.send_notify_to_queue(
                            queue_name=ingroup_job.queue_name, 
                            notifycation_type=NotificationTypeEnum.todo.value, 
                            payload=ingroup_job.id)
                    elif ingroup_job.status == JobStatusEnum.success:
                        successfull_jobs_count += 1
                    elif ingroup_job.status == JobStatusEnum.failed:
                        self.jobstore.set_status_for_job(job.id, JobStatusEnum.failed.value)

                if successfull_jobs_count == len(job.args):
                    self.jobstore.set_status_for_job(job.id, JobStatusEnum.success.value)
                    self.notify_jobs_waiting_for_this(job.id)

            elif job.queue_name == Pipeline.internal_queue_name:
                successfull_jobs_count = 0

                for inpipe_job_id in job.args: # но по порядку
                    inpipe_job = self.jobstore.get_job(inpipe_job_id)
                    if inpipe_job.status == JobStatusEnum.success:
                        successfull_jobs_count += 1
                        continue
                    elif inpipe_job.status == JobStatusEnum.created:
                        self.jobqueue.send_notify_to_queue(
                            queue_name=inpipe_job.queue_name, 
                            notifycation_type=NotificationTypeEnum.todo.value, 
                            payload=inpipe_job.id)
                    elif inpipe_job.status == JobStatusEnum.failed:
                        self.jobstore.set_status_for_job(job.id, JobStatusEnum.failed.value)
                    else:
                        break

                if successfull_jobs_count == len(job.args):
                    self.jobstore.set_status_for_job(job.id, JobStatusEnum.success.value)
                    self.notify_jobs_waiting_for_this(job.id)

            else:
                self._call_executor(job)

                self.jobstore.set_status_for_job(job_id, job.status)

                if job.status == JobStatusEnum.success:
                    if self.store_results and job.result is not None:
                        self.jobstore.save_result_for_job(job_id, job.get_result_bytes())

                    self.notify_jobs_waiting_for_this(job.id)
        else:
            logging.debug(f'seems like job {job_id} is already handled by other worker')

    def notify_jobs_waiting_for_this(self, job_id: str):
        for waiting_job_id, waiting_queue_name in self.jobstore.get_waiting_for_job(job_id):
            self.jobqueue.send_notify_to_queue(
                queue_name=waiting_queue_name, 
                notifycation_type=NotificationTypeEnum.todo.value, 
                payload=waiting_job_id)

    def _call_executor(self, job: Job):
        executor = self.executors[job.queue_name]

        logging.debug(f'starting job {job.id}')

        try:
            if isinstance(executor, Executor):
                executor.set_flexq_job_id(job.id)
                expected = tuple(executor.get_expected_exceptions())
                try:
                    job.result = executor.perform(*job.args, **job.kwargs)
                except expected as e:
                    logging.info(f'Caught expected exception in executor "{job.queue_name}", job_id={job.id}:\n{type(e).__name__}: {e}')
            else:
                job.result = executor(*job.args, **job.kwargs)

            job.status = JobStatusEnum.success.value
        except Exception as e:
            logging.info(f'Caught unexpected exception in executor "{job.queue_name}", job_id={job.id}:\n{type(e).__name__}: {e}')
            job.status = JobStatusEnum.failed.value
            
        logging.debug(f'finished job {job.id} with status: {job.status}')

    def wait_for_work(self):
        self.jobqueue.subscribe_to_queues(list(self.executors.keys()), self._todo_callback)

    def _todo_callback(self, job_name: str, job_id: str):
        raise NotImplemented

    def _abort_callback(self, job_id: str):
        raise NotImplemented

    def inspect_running_jobs(self):
        raise NotImplemented

    def start_running_jobs_inspector(self):
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }

        scheduler = BackgroundScheduler(job_defaults=job_defaults)

        scheduler.start()

        scheduler.add_job(self.inspect_running_jobs, 'interval', minutes=self.run_inspetion_every_n_minutes)
