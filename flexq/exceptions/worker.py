class WorkerException(Exception):
    pass

class JobExecutorExists(WorkerException):
    pass

class UnknownJobExecutor(WorkerException):
    pass

class RunningJobDuplicate(WorkerException):
    pass