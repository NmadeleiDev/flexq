import logging
from typing import Callable, Union
from flexq.executor import Executor
from flexq.exceptions.worker import JobExecutorExists, UnknownJobExecutor
from flexq.job import Group, Job, JobComposite, JobStatusEnum, Pipeline
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

    def _try_start_job(self, job_name: str, job_id: str):
        # пытаемся добавить работу в poll - есть успешно добавлась (т.е. ее там еще не было) , начинаем выполнять
        if job_name in (JobComposite.queue_name, Group.queue_name, Pipeline.queue_name):
            job = self.jobstore.get_job(job_id)

            if job.queue_name == Group.queue_name:
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
                        job.status = JobStatusEnum.failed.value
                        self.jobstore.set_status_for_job(job.id, job.status)

                if successfull_jobs_count == len(job.args):
                    job.status = JobStatusEnum.success.value
                    self.jobstore.set_status_for_job(job.id, job.status)

            elif job.queue_name == Pipeline.queue_name:
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
                        job.status = JobStatusEnum.failed.value
                        self.jobstore.set_status_for_job(job.id, job.status)
                    elif inpipe_job.status == JobStatusEnum.acknowledged:
                        pass                
                    break

                if successfull_jobs_count == len(job.args):
                    job.status = JobStatusEnum.success.value
                    self.jobstore.set_status_for_job(job.id, job.status)
            else:
                raise UnknownJobExecutor(f'Unknown composite type: {job.queue_name}')

        elif self.jobstore.try_acknowledge_job(job_id):
            logging.debug(f'Acknowledged job_id={job_id}')
            
            job = self.jobstore.get_job(job_id)

            self._call_executor(job)

            self.jobstore.set_status_for_job(job_id, job.status)

            if job.status == JobStatusEnum.success:
                if self.store_results and job.result is not None:
                    self.jobstore.save_result_for_job(job_id, job.get_result_bytes())

        else:
            logging.debug(f'seems like job {job_id} is already handled by other worker')
            return

        if job.parent_job_id is not None:
            self.jobqueue.send_notify_to_queue(
            queue_name=JobComposite.queue_name, 
            notifycation_type=NotificationTypeEnum.todo.value, 
            payload=job.parent_job_id)

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
                    logging.info(f'Caught expected exception in executor "{job.queue_name}", job_id={job.id}:{type(e).__name__}: {e}')
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
