class BrokerException(Exception):
    pass

class FailedToEnqueueJob(BrokerException):
    pass

class JobIdIsNone(BrokerException):
    pass