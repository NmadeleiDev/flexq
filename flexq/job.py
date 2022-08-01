from enum import Enum
from typing import Dict, Hashable, List

import pickle


class JobStatusEnum(str, Enum):
    created = 'created'
    acknowledged = 'acknowledged'
    finished = 'finished'

class Job:
    def __init__(self, queue_name: str, args: List = [], kwargs: Dict[str, Hashable] = {}, id=None, status=JobStatusEnum.created.value) -> None:
        self.queue_name = queue_name
        self.args = args
        self.kwargs = kwargs
        self.status = status

        self.id = id

        # if id is not None:
        #     if isinstance(id, str):
        #         self.id = id
        #     else:
        #         raise TypeError(f'id must be str or None, got {type(id)}')
        # else:
        #     self.id = uuid1().hex

    def get_args_bytes(self) -> bytes:
        return pickle.dumps(self.args)

    def get_kwargs_bytes(self) -> bytes:
        return pickle.dumps(self.kwargs)

    def set_args_bytes(self, val: bytes):
        val = pickle.loads(val)
        if not isinstance(val, list):
            raise TypeError(f'args must be list, but fould: {type(val)}')
        return val

    def set_kwargs_bytes(self, val: bytes):
        val = pickle.loads(val)
        if not isinstance(val, dict):
            raise TypeError(f'args must be dict, but fould: {type(val)}')
        return val