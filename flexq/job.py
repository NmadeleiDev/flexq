from __future__ import annotations
from datetime import datetime

from enum import Enum
from typing import Dict, Hashable, List, Union

import pickle

from flexq.exceptions.broker import JobIdIsNone


class JobStatusEnum(str, Enum):
    created = 'created'
    acknowledged = 'acknowledged'
    success = 'success'
    failed = 'failed'
    ephemeral = 'ephemeral'

class JobIntervalNameEnum(str, Enum):
    hours = 'hours'
    minutes = 'minutes'
    seconds = 'seconds'

class JobAbstract:
    def __init__(self, 
            id: Union[str, None]=None, 
            status:JobStatusEnum = JobStatusEnum.created.value, 
            parent_job_id: Union[str, None]=None,
            cron: Union[str, None] = None,
            interval_name: Union[JobIntervalNameEnum, None] = None,
            interval_value: int = 0,

            retry_until_success:bool=False,
            retry_delay_minutes:int=0,

            name: Union[str, None]=None,

            created_at: Union[datetime, None] = None,
            finished_at: Union[datetime, None] = None,

            last_heartbeat_ts: Union[datetime, None] = None,

            ) -> None:
        self.status = status

        self.id = id

        self.parent_job_id = parent_job_id
        
        self.cron = cron
        self.interval_name = interval_name
        self.interval_value = interval_value

        self.retry_until_success = retry_until_success
        self.retry_delay_minutes = retry_delay_minutes

        self.name = name

        self.created_at = created_at
        self.finished_at = finished_at

        self.last_heartbeat_ts = last_heartbeat_ts

        self.kwargs = {}
        self.args = []

        self.result = None

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
        return f'job_id={self.id}'

class Job(JobAbstract):
    def __init__(self, 
        queue_name: str, 
        args: List = [], 
        kwargs: Dict[str, Hashable] = {},
        result: any = None,

        **other_kwargs
        ) -> None:

        super().__init__(**other_kwargs)

        self.queue_name = queue_name
        self.args = args
        self.kwargs = kwargs

        self.result = result

    def __str__(self) -> str:
        return f'job_name={self.queue_name}, job_id={self.id}'

class JobComposite(JobAbstract):
    queue_name = '_flexq_job_composite'

    def __init__(self, 
        *jobs: Union[Job, Pipeline, Group], 
        broker_for_automatic_registering=None,
        
        **kwargs
        ) -> None:
        super().__init__(**kwargs)

        self.broker_for_automatic_registering = broker_for_automatic_registering

        self.kwargs = {}
        self.args = []
        
        self.broker_for_automatic_registering.register_job(self)
        for job in jobs:
            job.parent_job_id = self.id

            if job.id is None:
                if hasattr(self.broker_for_automatic_registering, 'register_job'):
                    self.broker_for_automatic_registering.register_job(job)
                else:
                    raise JobIdIsNone(f'Job passed to {type(self).__name__} must be registered (i.e. have an id) or broker_for_automatic_registering must be passed, which is not the case with job name = {job.queue_name}')
            else:
                if hasattr(self.broker_for_automatic_registering, 'register_job'):
                    self.broker_for_automatic_registering.jobstore.set_job_parent_id(job.id, job.parent_job_id)
                else:
                    raise JobIdIsNone(f'{type(self).__name__} needs broker_for_automatic_registering to be passed, as job {job} must be updated to have parent_job_id.')

            self.args.append(job.id)

    def __str__(self) -> str:
        return f'job_name={self.queue_name}, job_id={self.id}'

class Group(JobComposite):
    queue_name = '_flexq_group'

class Pipeline(JobComposite):
    queue_name = '_flexq_pipeline'
