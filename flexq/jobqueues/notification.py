from enum import Enum


class NotificationTypeEnum(str, Enum):
    todo = 'todo'
    done = 'done'
    abort = 'abort'

class Notification:
    def __init__(self, notification_type: NotificationTypeEnum, job_name: str, job_id: str) -> None:
        assert notification_type in NotificationTypeEnum

        self.notification_type = notification_type
        self.job_name = job_name
        self.job_id = job_id

    def __str__(self) -> str:
        return f'job_name={self.job_name}, job_id={self.job_id}, notification_type={self.notification_type}'