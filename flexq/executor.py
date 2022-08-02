from typing import Union


class Executor:
    def set_flexq_job_id(self, job_id: str):
        self.job_id = job_id

    def get_flexq_job_id(self) -> Union[str, None]:
        if hasattr(self, 'job_id'):
            return self.job_id
        else:
            return None