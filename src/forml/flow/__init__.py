"""

rfc = Trans() >> RFC()
NO SOURCE or SINK
ensembled = Transformer(a=1, b=10) >> Ensembler(estimators=(rfc, XGB()), folds=2)


pipe = Pipeline('blabla', etl='asd').


@pipe.decor
def xxx():
    ...


Pipeline(
    name=...,
    etl=...
    flow=...,
    crossval=...,
    label=...,
    schedule=...
    report=...,
)


src/
   ..../
       myprj/
        __init__.py
           pipe1/
              __init__.py
           pipe2/
              __init__.py


myprj.PIPELINES = ('pipe1', 'pipe2')


myprj.pipe1:
PIPELINE = Transformer(...) >> Ensembler(...)

class Pipeline:



class MySolution(Pipeline):
    ETL = ...


    def plan(self):

"""

import abc

from forml.flow import operator, task, segment
from forml.flow.graph import node


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """

    def __rshift__(self, right: 'Operator') -> segment.Link:
        """Semantical composition construct.
        """
        return segment.Link(right, segment.Recursive(self, segment.Origin()))

    @abc.abstractmethod
    def compose(self, left: segment.Builder) -> segment.Segment:
        """Expand the left segment.

        Returns: Operator composition plan.
        """


class Source:
    def __init__(self):
        self._apply = ...
        self._train = ...


class Composer:
    def __init__(self):
        self._id: str = ...
        self._flow: segment.Segment = ...
        self._source: Source = ...
        # self._label: node.Worker = ... #(splitter 1:2) ???
        self._score = ...  # cv+metric -> single number
        self._report = ...  # arbitrary metrics -> kv list

    @property
    def train(self) -> node.Compound:
        """Training graph.
        """
        return

    @property
    def apply(self):
        return

    @property
    def tune(self):
        return

    @property
    def score(self):

    @property
    def report(self):
