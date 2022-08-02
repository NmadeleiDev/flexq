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
    def add_job_to_queue(self, job: Job) -> bool:
        raise NotImplemented

    def get_job(self, job_id: str, include_result=False) -> Job:
        raise NotImplemented

    # вызывается воркером в inspect_running_jobs если есть возможность взять еще задачи
    def get_not_acknowledged_jobs_ids_in_queues(self, queues_names: str) -> List[Tuple[str, str]]:
        raise NotImplemented

    # вызывается воркером после заверешния работы, чтобы вызвать другие работу, ожиадющие завершенной
    def get_waiting_for_job(self, job_id: str) -> List[Tuple[str, str]]:
        raise NotImplemented
