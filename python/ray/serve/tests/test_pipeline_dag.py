import pytest
import os
import sys
from typing import TypeVar, Any

import numpy as np

import ray
from ray import serve
from ray.serve.api import RayServeDAGHandle
from ray.experimental.dag.input_node import InputNode
from ray.serve.pipeline.api import build as pipeline_build

RayHandleLike = TypeVar("RayHandleLike")
NESTED_HANDLE_KEY = "nested_handle"


@serve.deployment
class ClassHello:
    def __init__(self):
        pass

    def hello(self):
        return "hello"


@serve.deployment
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        return self.ratio * self.weight * input

    def __call__(self, request):
        input_data = request
        return self.ratio * self.weight * input_data


@serve.deployment
class Combine:
    def __init__(
        self,
        m1: "RayHandleLike",
        m2: "RayHandleLike" = None,
        m2_nested: bool = False,
    ):
        self.m1 = m1
        self.m2 = m2.get(NESTED_HANDLE_KEY) if m2_nested else m2

    def __call__(self, req):
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        return sum(ray.get([r1_ref, r2_ref]))


@serve.deployment
class Counter:
    def __init__(self, val):
        self.val = val

    def get(self):
        return self.val

    def inc(self, inc):
        self.val += inc


@serve.deployment
def fn_hello():
    return "hello"


@serve.deployment
def combine(m1_output, m2_output, kwargs_output=0):
    return m1_output + m2_output + kwargs_output


def class_factory():
    class MyInlineClass:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    return MyInlineClass


@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def forward(self, inp: int) -> int:
        print(f"Adder got {inp}")
        return inp + self.increment

    __call__ = forward


@serve.deployment
class Driver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self, inp: Any) -> Any:
        print(f"Driver got {inp}")
        return await self.dag.remote(inp)


@serve.deployment
class NoargDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await self.dag.remote()


def test_single_func_no_input(serve_instance):
    dag = fn_hello.bind()
    serve_dag = NoargDriver.bind(dag)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote()) == "hello"


def test_single_func_deployment_dag(serve_instance):
    with InputNode() as dag_input:
        dag = combine.bind(dag_input[0], dag_input[1], kwargs_output=1)
        serve_dag = Driver.bind(dag)
    handle = serve.run(serve_dag)
    assert ray.get(handle.remote([1, 2])) == 4


def test_simple_class_with_class_method(serve_instance):
    with InputNode() as dag_input:
        model = Model.bind(2, ratio=0.3)
        dag = model.forward.bind(dag_input)
        serve_dag = Driver.bind(dag)
    handle = serve.run(serve_dag)
    assert ray.get(handle.remote(1)) == 0.6


def test_func_class_with_class_method(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.bind(m1_output, m2_output, kwargs_output=dag_input[2])
        serve_dag = Driver.bind(combine_output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote([1, 2, 3])) == 8


def test_multi_instantiation_class_deployment_in_init_args(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2=m2)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = Driver.bind(combine_output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote(1)) == 5


def test_shared_deployment_handle(serve_instance):
    with InputNode() as dag_input:
        m = Model.bind(2)
        combine = Combine.bind(m, m2=m)
        combine_output = combine.__call__.bind(dag_input)
        serve_dag = Driver.bind(combine_output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote(1)) == 4


def test_multi_instantiation_class_nested_deployment_arg_dag(serve_instance):
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        output = combine.__call__.bind(dag_input)
        serve_dag = Driver.bind(output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote(1)) == 5


def test_class_factory(serve_instance):
    with InputNode() as _:
        instance = ray.remote(class_factory()).bind(3)
        output = instance.get.bind()
        serve_dag = NoargDriver.bind(output)

    handle = serve.run(serve_dag)
    assert ray.get(handle.remote()) == 3


@serve.deployment
class Echo:
    def __init__(self, s: str):
        self._s = s

    def __call__(self, *args):
        return self._s


def test_single_node_deploy_success(serve_instance):
    m1 = Adder.bind(1)
    handle = serve.run(m1)
    assert ray.get(handle.remote(41)) == 42


def test_single_node_driver_sucess(serve_instance):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with InputNode() as input_node:
        out = m1.forward.bind(input_node)
        out = m2.forward.bind(out)
    driver = Driver.bind(out)
    handle = serve.run(driver)
    assert ray.get(handle.remote(39)) == 42


def test_options_and_names(serve_instance):

    m1 = Adder.bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.name == "Adder"

    m1 = Adder.options(name="Adder2").bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.name == "Adder2"

    m1 = Adder.options(num_replicas=2).bind(1)
    m1_built = pipeline_build(m1)[-1]
    assert m1_built.num_replicas == 2


@serve.deployment
class TakeHandle:
    def __init__(self, handle) -> None:
        self.handle = handle

    def __call__(self, inp):
        return ray.get(self.handle.remote(inp))


def test_passing_handle(serve_instance):
    child = Adder.bind(1)
    parent = TakeHandle.bind(child)
    driver = Driver.bind(parent)
    handle = serve.run(driver)
    assert ray.get(handle.remote(1)) == 2


def test_passing_handle_in_obj(serve_instance):
    @serve.deployment
    class Parent:
        def __init__(self, d):
            self._d = d

        async def __call__(self, key):
            return await self._d[key].remote()

    child1 = Echo.bind("ed")
    child2 = Echo.bind("simon")
    parent = Parent.bind({"child1": child1, "child2": child2})

    handle = serve.run(parent)
    assert ray.get(handle.remote("child1")) == "ed"
    assert ray.get(handle.remote("child2")) == "simon"


def test_pass_handle_to_multiple(serve_instance):
    @serve.deployment
    class Child:
        def __call__(self, *args):
            return os.getpid()

    @serve.deployment
    class Parent:
        def __init__(self, child):
            self._child = child

        def __call__(self, *args):
            return ray.get(self._child.remote())

    @serve.deployment
    class GrandParent:
        def __init__(self, child, parent):
            self._child = child
            self._parent = parent

        def __call__(self, *args):
            # Check that the grandparent and parent are talking to the same child.
            assert ray.get(self._child.remote()) == ray.get(self._parent.remote())
            return "ok"

    child = Child.bind()
    parent = Parent.bind(child)
    grandparent = GrandParent.bind(child, parent)

    handle = serve.run(grandparent)
    assert ray.get(handle.remote()) == "ok"


def test_non_json_serializable_args(serve_instance):
    # Test that we can capture and bind non-json-serializable arguments.
    arr1 = np.zeros(100)
    arr2 = np.zeros(200)

    @serve.deployment
    class A:
        def __init__(self, arr1):
            self.arr1 = arr1
            self.arr2 = arr2

        def __call__(self, *args):
            return self.arr1, self.arr2

    handle = serve.run(A.bind(arr1))
    ret1, ret2 = ray.get(handle.remote())
    assert np.array_equal(ret1, arr1) and np.array_equal(ret2, arr2)


# TODO: check that serve.build raises an exception.


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
