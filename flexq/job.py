from enum import Enum
from typing import Dict, Hashable, List, Union

import pickle


class JobStatusEnum(str, Enum):
    created = 'created'
    acknowledged = 'acknowledged'
    finished = 'finished'

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