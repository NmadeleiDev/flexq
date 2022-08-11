import logging
from threading import Lock
from typing import List, Type, Union

from .jobstores.jobstore_base import JobStoreBase

class Executor:
    set_origin_job_id = False
    jobstore: Union[JobStoreBase, None] = None

    def set_jobstore(self, jobstore: JobStoreBase):
        Executor.jobstore = jobstore

    def set_flexq_job_id(self, job_id: str):
        self.job_id = job_id

    def get_flexq_job_id(self) -> Union[str, None]:
        if hasattr(self, 'job_id'):
            return self.job_id
        else:
            return None

    def set_flexq_origin_job_id(self, origin_job_id: str):
        self.origin_job_id = origin_job_id

    def get_flexq_origin_job_id(self) -> Union[str, None]:
        if hasattr(self, 'origin_job_id'):
            return self.origin_job_id
        else:
            return None

    def set_state(self, state_msg: str, use_this_job_id=True, use_origin_job_id=False):
        if self.jobstore is None:
            logging.debug(f'job_id {self.get_flexq_job_id()} cant log state, as jobstore is None. State: {state_msg}')
            return
        if use_this_job_id:
            self.jobstore.set_job_user_status(self.get_flexq_job_id(), state_msg)
        if use_origin_job_id:
            self.jobstore.set_job_user_status(self.get_flexq_origin_job_id(), state_msg)

    # user-defined methods
    def get_expected_exceptions(self) -> List[Type[Exception]]:
        return []

    def perform(self, *args, **kwargs):
        pass