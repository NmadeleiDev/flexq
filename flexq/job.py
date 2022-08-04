from enum import Enum
from typing import Dict, Hashable, List, Union
from __future__ import annotations

import pickle
from flexq.broker import Broker

from flexq.exceptions.broker import JobIdIsNone
from flexq.jobstores.jobstore_base import JobStoreBase


class JobStatusEnum(str, Enum):
    created = 'created'
    acknowledged = 'acknowledged'
    success = 'success'
    failed = 'failed'


class Job:
    def __init__(self, queue_name: str, args: List = [], kwargs: Dict[str, Hashable] = {}, id=None, status=JobStatusEnum.created.value, result=any, start_after_job_id: Union[str, None]=None) -> None:
        self.queue_name = queue_name
        self.args = args
        self.kwargs = kwargs
        self.status = status

        self.id = id

        self.result = result

        self.start_after_job_id = start_after_job_id

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

class JobComposite:
    def __init__(self, *jobs: Union[Job, Pipeline, Group], start_after_job_id: Union[str, None]=None, id=None, broker_for_automatic_registering: Union[Broker, None]=None) -> None:
        self.start_after_job_id = start_after_job_id
        self.queue_name = self.internal_queue_name
        self.id = id

        self.broker_for_automatic_registering = broker_for_automatic_registering

        self.kwargs = {}
        self.args = []
        for job in jobs:
            if job.id is None:
                if isinstance(self.broker_for_automatic_registering, Broker):
                    self.broker_for_automatic_registering.register_job(job)
                else:
                    raise JobIdIsNone(f'Job passed to {type(self).__name__} must be regitered (i.e. have an id) or broker_for_automatic_registering must be passed, which is not the case with job name = {job.queue_name}')

            self.args.append(job.id)

class Group(JobComposite, Job):
    internal_queue_name = '_flexq_group'

class Pipeline(JobComposite, Job):
    internal_queue_name = '_flexq_pipeline'
