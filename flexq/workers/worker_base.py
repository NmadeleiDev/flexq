import logging
from typing import Callable, Union
from flexq.executor import Executor
from flexq.exceptions.worker import JobExecutorExists
from flexq.job import Job, JobStatusEnum
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
            self.jobstore.set_status_for_job(job_id, JobStatusEnum.acknowledged.value)

            job = self.jobstore.get_job(job_id)
            self._call_executor(job)

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

    def _call_executor(self, job: Job):
        executor = self.executors[job.queue_name]

        logging.debug(f'starting job {job.id}')

        if isinstance(executor, Executor):
            executor.set_flexq_job_id(job.id)
            expected = tuple(executor.get_expected_exceptions())
            try:
                job.result = executor.perform(*job.args, **job.kwargs)
            except expected as e:
                logging.info(f'Caught exception in executor {job.queue_name}, job_id={job.id}: {e}')
        else:
            job.result = executor(*job.args, **job.kwargs)

        logging.debug(f'finished job {job.id}')

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
