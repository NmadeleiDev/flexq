from typing import List, Tuple
from flexq.job import Job, JobStatusEnum


class JobStoreBase:
    def __init__(self) -> None:
        pass

    # вызывается воркером после получения работы в _todo_callback
    def try_acknowledge_job(self, job_id: str) -> bool:
        raise NotImplemented

    # вызывается воркером после завершения работы
    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        raise NotImplemented

    # вызывается воркером после завершения работы при наличии возврата и store_results=True
    def save_result_for_job(self, job_id: str, result: bytes) -> None:
        raise NotImplemented

    # вызывается брокером при добавлении задачи клиентом
    def add_job_to_store(self, job: Job) -> str:
        raise NotImplemented

    def update_job_in_store(self, job: Job):
        raise NotImplemented

    def remove_job_from_store(self, job_id: str):
        raise NotImplemented

    def get_child_job_ids(self, parent_job_id: str) -> List[str]:
        raise NotImplemented

    def get_job(self, job_id: str, include_result=False) -> Job:
        raise NotImplemented

    def get_not_acknowledged_jobs_ids_and_queue_names(self) -> List[Tuple[str, str]]:
        raise NotImplemented

