from abc import ABC
from datetime import datetime
from typing import List, Tuple, Union, Optional
from flexq.job import Job, JobStatusEnum


class JobStoreBase(ABC):
    def __init__(self, instance_name='default') -> None:
        """
        It's important that JobStore object do not create anything unpickable in __init__. Creation of all connections must be in self.init_conn
        """
        self.instance_name = str(instance_name)

    def init_conn(self):
        pass

    # вызывается воркером после получения работы в _todo_callback
    def try_acknowledge_job(self, job_id: str, worker_heartbeat_interval_seconds: int) -> bool:
        pass

    # вызывается воркером после завершения работы
    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        pass

    # вызывается воркером после завершения работы при наличии возврата и store_results=True
    def save_result_for_job(self, job_id: str, result: bytes) -> None:
        pass

    # вызывается брокером при добавлении задачи клиентом
    def add_job_to_store(self, job: Job) -> str:
        pass

    def remove_job_from_store(self, job_id: str):
        pass

    def get_child_job_ids(self, parent_job_id: str) -> List[str]:
        pass

    def get_jobs(self, job_id: Optional[str] = None, include_result=False, with_schedule_only=False,
                 retry_until_success_only=False,
                 heartbeat_missed_by_more_than_n_seconds: Optional[int] = None,
                 status: Optional[JobStatusEnum] = None) -> Optional[List[Job]]:
        pass

    def get_job_user_status(self, job_id: str) -> str:
        pass

    def set_job_user_status(self, job_id: str, value: str):
        pass

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        pass

    def set_job_parent_id(self, job_id: str, parent_job_id: str):
        pass

    def set_job_last_heartbeat_ts_to_now(self, job_id: str, set_start_ts_also: bool = False):
        pass

    def set_job_start_ts(self, job_id: str, start_timestamp: datetime):
        pass
