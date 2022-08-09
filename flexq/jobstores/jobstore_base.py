from typing import List, Tuple
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

    def update_job_in_store(self, job: Job):
        pass

    def remove_job_from_store(self, job_id: str):
        pass

    def get_child_job_ids(self, parent_job_id: str) -> List[str]:
        pass

    def get_job(self, job_id: str, include_result=False) -> Job:
        pass

    def get_job_user_status(self, job_id: str) -> str:
        pass

    def set_job_user_status(self, job_id: str, value: str):
        pass

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        pass

