from typing import List, Tuple, Union
from flexq.job import Job, JobStatusEnum


class JobStoreBase:
    def __init__(self) -> None:
        pass

    # вызывается воркером после получения работы в _todo_callback
    def try_acknowledge_job(self, job_id: str) -> bool:
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

    def get_jobs(self, job_id: Union[str, None] = None, include_result=False, with_schedule_only=False, retry_until_success_only=False, last_heartbeat_ts_more_than_n_minutes_ago:Union[int, None] = None) -> Union[List[Job], None]:
        pass

    def get_job_user_status(self, job_id: str) -> str:
        pass

    def set_job_user_status(self, job_id: str, value: str):
        pass

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        pass

    def set_job_parent_id(self, job_id: str, parent_job_id: str):
        pass

    def set_job_last_heartbeat_ts_to_now(self, job_id: str):
        pass
