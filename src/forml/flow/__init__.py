import abc
import collections

import pandas

from forml.flow.graph import node


class Plan(collections.namedtuple('Plan', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    def __init__(self):
        self.left: Operator = Stub()

    def __rshift__(self, right) -> 'Operator':
        """Semantical construct for operator composition.
        """

    @abc.abstractmethod
    def plan(self) -> Plan:
        """Create and return new plan for this operator composition.

        Returns: Operator composition plan.
        """


class Stub(Operator):
    def plan(self) -> Plan:
        return Plan(node.Condensed(node.Future()),
                    node.Condensed(node.Future()),
                    node.Condensed(node.Future()))


class Pipeline(Operator):

    def train(self) -> 'Pipeline':
        """Train the pipeline.
        """

    def apply(self) -> pandas.DataFrame:
        """
        """
