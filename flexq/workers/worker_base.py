import logging
from typing import Callable, Union
from flexq.executor import Executor
from flexq.exceptions.worker import JobExecutorExists
from flexq.job import Job
from flexq.jobqueues.jobqueue_base import JobQueueBase
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

    def _call_executor(self, job: Job):
        executor = self.executors[job.queue_name]

        logging.debug(f'starting job {job.id}')

        if isinstance(executor, Executor):
            executor.set_flexq_job_id(job.id)
            job.result = executor.perform(*job.args, **job.kwargs)
        else:
            job.result = executor(*job.args, **job.kwargs)

        logging.debug(f'finished job {job.id}')
        

    def wait_for_work(self):
        self.jobqueue.subscribe_to_queues(list(self.executors.keys()), self._todo_callback)

    def _todo_callback(self, job_name: str, job_id: str):
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
