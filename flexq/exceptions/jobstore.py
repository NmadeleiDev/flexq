class JobStoreException(Exception):
    pass


class JobNotFoundInStore(JobStoreException):
    pass


class CanNotDeleteAwaitedForJob(JobStoreException):
    pass
