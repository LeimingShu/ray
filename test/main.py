"""
任务的分发在 _raylet CoreWorker 中
根据索引获取任务的值也在 _raylet CoreWorker 中
"""

import ray
from ray.remote_function import RemoteFunction
import logging
from functools import wraps

from ray import cloudpickle as pickle
from ray import ray_constants
from ray.function_manager import FunctionDescriptor
import ray.signature

ray.init()

# Default parameters for remote functions.
DEFAULT_REMOTE_FUNCTION_CPUS = 1
DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS = 1
DEFAULT_REMOTE_FUNCTION_MAX_CALLS = 0
# Normal tasks may be retried on failure this many times.
# TODO(swang): Allow this to be set globally for an application.
DEFAULT_REMOTE_FUNCTION_NUM_TASK_RETRIES = 3

logger = logging.getLogger(__name__)


class RemoteFunctionImp(RemoteFunction):
    def __init__(self, function, num_cpus, num_gpus, memory, object_store_memory, resources, num_return_vals, max_calls,
                 max_retries):
        super().__init__(function, num_cpus, num_gpus, memory, object_store_memory, resources, num_return_vals,
                         max_calls, max_retries)
        print("self._function_name = {}".format(self._function_name))
        print("self._decorator = {}".format(self._decorator))
        print("self._function_signature = {}".format(self._function_signature))
        worker = ray.worker.get_global_worker()
        worker.check_connected()


def hello(function):
    return RemoteFunctionImp(function, num_cpus=None, num_gpus=None, memory=None,
                             object_store_memory=None, resources=None, num_return_vals=None, max_calls=None,
                             max_retries=None)


@hello
def counter(x, y):
    return (x + 1) ** y




if __name__ == '__main__':
    f = counter.remote(1, 2)
    r = ray.get(f)
    print("计算 counter.remote(0) = {}。".format(r))

