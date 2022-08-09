from __future__ import annotations

from enum import Enum
from typing import Dict, Hashable, List, Union

import pickle

from flexq.exceptions.broker import JobIdIsNone


class JobStatusEnum(str, Enum):
    created = 'created'
    acknowledged = 'acknowledged'
    success = 'success'
    failed = 'failed'

class JobIntervalNameEnum(str, Enum):
    hours = 'hours'
    minutes = 'minutes'
    seconds = 'seconds'


class Job:
    def __init__(self, 
            queue_name: str, 
            args: List = [], 
            kwargs: Dict[str, Hashable] = {}, 
            id=None, 
            status=JobStatusEnum.created.value, 
            result=any, 
            parent_job_id: Union[str, None]=None,
            cron: Union[str, None] = None,
            interval_name: Union[JobIntervalNameEnum, None] = None,
            interval_value: int = 0,

            retry_until_success=False,
            retry_delay_minutes=0,

            name=None,

            ) -> None:
        self.queue_name = queue_name
        self.args = args
        self.kwargs = kwargs
        self.status = status

        self.id = id

        self.result = result

        self.parent_job_id = parent_job_id
        
        self.cron = cron
        self.interval_name = interval_name
        self.interval_value = interval_value

        self.retry_until_success = retry_until_success
        self.retry_delay_minutes = retry_delay_minutes

        self.name = name

    def get_args_bytes(self) -> bytes:
        return pickle.dumps(self.args)

    def get_kwargs_bytes(self) -> bytes:
        return pickle.dumps(self.kwargs)

    def get_result_bytes(self) -> bytes:
        return pickle.dumps(self.result)

    def set_args_bytes(self, val: bytes):
        val = pickle.loads(val)
        if not isinstance(val, (list, tuple)):
            raise TypeError(f'args must be list, but fould: {type(val)}')
        self.args = val

    def set_kwargs_bytes(self, val: bytes):
        val = pickle.loads(val)
        if not isinstance(val, dict):
            raise TypeError(f'args must be dict, but fould: {type(val)}')
        self.kwargs = val

    def set_result_bytes(self, val: bytes):
        val = pickle.loads(val)
        self.result = val

    def __str__(self) -> str:
        return f'job_name={self.queue_name}, job_id={self.id}'

class JobComposite:
    queue_name = '_flexq_job_composite'

    def __init__(self, 
            *jobs: Union[Job, Pipeline, Group], 
            parent_job_id: Union[str, None]=None, 
            id=None, 
            broker_for_automatic_registering=None,

            cron: Union[str, None] = None,
            interval_name: Union[JobIntervalNameEnum, None] = None,
            interval_value: int = 0,

        ) -> None:
        self.parent_job_id = parent_job_id
        self.id = id

        self.cron = cron
        self.interval_name = interval_name
        self.interval_value = interval_value

        self.broker_for_automatic_registering = broker_for_automatic_registering

        self.kwargs = {}
        self.args = []
        self.broker_for_automatic_registering.register_job(self)
        for job in jobs:
            job.parent_job_id = self.id # чтобы scheduler не начинал самостоятельно эти работы
            if job.id is None:
                if hasattr(self.broker_for_automatic_registering, 'register_job'):
                    self.broker_for_automatic_registering.register_job(job)
                else:
                    raise JobIdIsNone(f'Job passed to {type(self).__name__} must be regitered (i.e. have an id) or broker_for_automatic_registering must be passed, which is not the case with job name = {job.queue_name}')

            self.args.append(job.id)
        # self.broker_for_automatic_registering.jobstore.update_job_in_store(self)

class Group(JobComposite, Job):
    queue_name = '_flexq_group'

class Pipeline(JobComposite, Job):
    queue_name = '_flexq_pipeline'
