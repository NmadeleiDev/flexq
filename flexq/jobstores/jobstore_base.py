from typing import List, Tuple
from flexq.job import Job, JobStatusEnum


class JobStoreBase:
    def __init__(self) -> None:
        pass

    def try_acknowledge_job(self, job_id: str) -> bool:
        # у этой операции должена быть очень низкая задержка
        raise NotImplemented

    def set_status_for_job(self, job_id: str, status: JobStatusEnum) -> None:
        raise NotImplemented

    def add_job_to_queue(self, job: Job) -> bool:
        raise NotImplemented

    def get_job(self, job_id: str) -> Job:
        raise NotImplemented

    def get_jobs_ids_in_queues_waiting_for_job(self, job_id: str, queues_names: str) -> List[str]:
        raise NotImplemented

    def get_jobs_ids_and_queue_names_waiting_for_job(self, job_id: str) -> List[Tuple[str, str]]:
        raise NotImplemented

    def get_queue_names_interested_in_job(self, job_id: str) -> List[str]:
        raise NotImplemented
