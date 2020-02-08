"""
Testing framework.
"""
import typing

from forml.flow.pipeline import topology
from forml.testing import spec, routine


def operator(subject: typing.Type[topology.Operator]) -> typing.Type[routine.Suite]:
    """Operator base class generator.

    Args:
        subject: Operator to be tested within given suite.
    """
    class Operator(routine.Suite, metaclass=routine.Meta):
        """Generated base class.
        """
        @property
        def __operator__(self) -> typing.Type[topology.Operator]:
            """Attached operator.

            Returns: Operator instance.
            """
            return subject
    return Operator


class Case(spec.Appliable):
    """Test case entrypoint.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(spec.Scenario.Params(*args, **kwargs))

    def train(self, features: typing.Any, labels: typing.Any = None) -> spec.Trained:
        """Train input dataset definition.
        """
        return spec.Trained(self._params, spec.Scenario.Input(train=features, label=labels))
