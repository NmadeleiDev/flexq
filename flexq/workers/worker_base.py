from copy import copy
import logging
from multiprocessing import Process
from threading import Thread
from time import sleep
from typing import Callable, Union
from flexq.executor import Executor
from flexq.exceptions.worker import JobExecutorExists, UnknownJobExecutor
from flexq.job import Group, Job, JobComposite, JobStatusEnum, Pipeline
from flexq.jobqueues.jobqueue_base import JobQueueBase, NotificationTypeEnum
from flexq.jobstores.jobstore_base import JobStoreBase

import traceback

class WorkerBase:
    def __init__(self, jobstore: JobStoreBase, jobqueue: JobQueueBase, max_parallel_executors:Union[int, None]=None, store_results=True, update_heartbeat_interval_seconds=60, n_processes=1) -> None:
        self.executors = {}
        self.running_jobs = set([])

        self.jobstore = jobstore
        self.jobqueue = jobqueue

        self.store_results = store_results
        self.max_parallel_executors = max_parallel_executors

        self.update_heartbeat_interval_seconds = update_heartbeat_interval_seconds

        self.n_processes = n_processes

    def _before_start_routine(self):
        self.jobstore.init_conn()
        self.jobqueue.init_conn()

    def _acquire_lock(self):
        pass

    def _release_lock(self):
        pass

    def add_job_executor(self, name: str, cb: Union[Callable, Executor]):
        if name not in self.executors.keys():
            self.executors[name] = cb
        else:
            raise JobExecutorExists(f'Job executor name "{name}" exists. Add executor with other name.')

    def _try_start_job(self, job_name: str, job_id: str):
        # пытаемся добавить работу в poll - есть успешно добавлась (т.е. ее там еще не было) , начинаем выполнять

        if job_name in (JobComposite.queue_name, Group.queue_name, Pipeline.queue_name):
            job = self.jobstore.get_jobs(job_id)[0]
            if job.status == JobStatusEnum.ephemeral.value:
                logging.debug(f'job {job} is ephemeral, skipping')
                return

            self._add_running_job(job_id)

            if job.queue_name == Group.queue_name:
                successfull_jobs_count = 0
                child_jobs = self.jobstore.get_child_job_ids(job.id)

                for ingroup_job_id in child_jobs:
                    ingroup_job = self.jobstore.get_jobs(ingroup_job_id)[0]
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

                if successfull_jobs_count == len(child_jobs):
                    job.status = JobStatusEnum.success.value
                    self.jobstore.set_status_for_job(job.id, job.status)

            elif job.queue_name == Pipeline.queue_name:
                successfull_jobs_count = 0
                child_jobs = self.jobstore.get_child_job_ids(job.id)

                for inpipe_job_id in child_jobs:
                    inpipe_job = self.jobstore.get_jobs(inpipe_job_id)[0]
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

                if successfull_jobs_count == len(child_jobs):
                    job.status = JobStatusEnum.success.value
                    self.jobstore.set_status_for_job(job.id, job.status)
            else:
                self._remove_running_job(job_id)
                raise UnknownJobExecutor(f'Unknown composite type: {job.queue_name}')

            self._remove_running_job(job_id)
        elif self.jobstore.try_acknowledge_job(job_id, self.update_heartbeat_interval_seconds):
            logging.debug(f'Acknowledged job_id={job_id}')
            self._add_running_job(job_id)
            self.jobstore.set_job_last_heartbeat_ts_to_now(job_id)
            
            job = self.jobstore.get_jobs(job_id)[0]

            self._call_executor(job)

            if job.status == JobStatusEnum.success:
                if self.store_results and job.result is not None:
                    self.jobstore.save_result_for_job(job_id, job.get_result_bytes())

            self.jobstore.set_status_for_job(job_id, job.status)

            self._remove_running_job(job_id)
        else:
            logging.debug(f'seems like job {job_id} is already handled by other worker')
            return

        if job.status == JobStatusEnum.success.value and job.parent_job_id is not None:
            self.jobqueue.send_notify_to_queue(
            queue_name=JobComposite.queue_name, 
            notifycation_type=NotificationTypeEnum.todo.value, 
            payload=job.parent_job_id)

    def _add_running_job(self, job_id: str):
        self._acquire_lock()
        self.running_jobs.add(job_id)
        self._release_lock()

    def _remove_running_job(self, job_id: str):
        self._acquire_lock()
        if job_id in self.running_jobs:
            self.running_jobs.remove(job_id)
        else:
            logging.debug(f'tryied to remove job_id={job_id}, but it is not in self.running_jobs')
        self._release_lock()

    def _get_origin_job_id(self, job: Job) -> str:
        parent_job = job
        while parent_job.parent_job_id is not None:
            parent_job = self.jobstore.get_jobs(parent_job.parent_job_id)[0]
        return parent_job.id

    def _call_executor(self, job: Job):
        executor = self.executors[job.queue_name]

        logging.debug(f'starting job {job.id}')

        try:
            if isinstance(executor, type(Executor)):
                executor = executor()
                executor.set_flexq_job_id(job.id)
                executor.set_jobstore(self.jobstore)
                
                if executor.set_origin_job_id:
                    executor.set_flexq_origin_job_id(self._get_origin_job_id(job))

                expected_exceptions = tuple(executor.get_expected_exceptions())
                try:
                    result = executor.perform(*job.args, **job.kwargs)
                except expected_exceptions as e:
                    traceback_str = ''.join(traceback.format_tb(e.__traceback__))

                    logging.info(f'Caught expected exception in executor "{job.queue_name}", job_id={job.id}:{type(e).__name__}: {e}, traceback: {traceback_str}')
                    result = None
            else:
                result = executor(*job.args, **job.kwargs)

            if result is not None:
                job.result = result
            job.status = JobStatusEnum.success.value
        except Exception as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))

            logging.info(f'Caught unexpected exception in executor "{job.queue_name}", job_id={job.id}:\n{type(e).__name__}: {e}, traceback: {traceback_str}')
            job.status = JobStatusEnum.failed.value

        logging.debug(f'job {job} execution completed with status: {job.status}')

    def _start_routine(self):
        Thread(target=self.start_updating_heartbeat, daemon=True).start()

        self.jobqueue.subscribe_to_queues(list(self.executors.keys()), self._todo_callback)

    def _wait_for_work(self, process_idx=None):
        logging.debug(f'starting work in process_idx={process_idx}')
        self._before_start_routine()
        self._start_routine()

    def wait_for_work(self):
        if self.n_processes < 1:
            raise ValueError(f'n_processes can not be less than 1 (passed {self.n_processes})')
        if self.n_processes > 1:
            for i in range(self.n_processes - 1):
                Process(target=self._wait_for_work, kwargs={'process_idx', i}).start()

        Process(target=self._wait_for_work).start()

    def start_updating_heartbeat(self):
        while True:
            self._acquire_lock()
            running_jobs = copy(self.running_jobs)
            self._release_lock()

            for job_id in running_jobs:
                self.jobstore.set_job_last_heartbeat_ts_to_now(job_id)

            sleep(self.update_heartbeat_interval_seconds)

    def _todo_callback(self, job_name: str, job_id: str):
        pass

    def _abort_callback(self, job_id: str):
        """
        Planned to be implemented
        """
        pass

    

