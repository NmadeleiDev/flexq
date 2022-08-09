from typing import List, Type, Union

class Executor:
    set_origin_job_id = False
    save_state_cb = lambda job_id, msg: None

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
        if use_this_job_id:
            self.save_state_cb(self.get_flexq_job_id(), state_msg)
        if use_origin_job_id:
            self.save_state_cb(self.get_flexq_origin_job_id(), state_msg)

    # user-defined methods
    def get_expected_exceptions(self) -> List[Type[Exception]]:
        return []

    def perform(self, *args, **kwargs):
        pass