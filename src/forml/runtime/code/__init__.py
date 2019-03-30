import abc
import collections
import typing

from forml.flow import task
from forml.flow.graph import view, node as grnode


class Result(collections.namedtuple('Result', 'state, values')):
    pass


class Output(collections.namedtuple('Output', 'task, index')):
    def __new__(cls, task: int, index: typing.Optional[int] = None):
        return super().__new__(cls, task, index)


class Input(collections.namedtuple('Input', 'state, params, values')):
    pass


class Functor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __call__(self, *args) -> typing.Any:
        ...

class Task(collections.namedtuple('Task', 'key, function, input')):
    """


    function:
        instantiate actor
        load state
        call actor entrypoint with inputs
        if fit then dump state
    """


class Visitor(view.Visitor):

    def visit_node(self, node: grnode.Atomic) -> None:
        # wrap in instantiator
        #

    @abc.abstractmethod
    def visit_task(self, task: Task) -> None:
        pass


class Driver:

    def training(self, visitor) -> None:
        """ports: ordinal, *states
        """
        path = ...
        path.accept(visitor)

        lineage.put(record)
