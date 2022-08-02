class JobStoreException(Exception):
    pass

class JobNotFoundInStore(JobStoreException):
    pass