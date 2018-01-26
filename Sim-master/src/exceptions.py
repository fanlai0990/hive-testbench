"""
exceptions
~~~~~~~~~~~~~~~~~~~
This module contains the set of Customized exceptions.
"""


class OptimizerException(Exception):
    pass


class SchedulerException(Exception):
    pass


class AllocationException(SchedulerException):
    pass


class SortCoflowException(SchedulerException):
    def __init__(self, coflow_id):
        self.coflow_id = coflow_id
