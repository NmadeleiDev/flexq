class BrokerException(Exception):
    pass

class FailedToEnqueueJob(BrokerException):
    pass