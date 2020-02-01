"""
Testing framework.
"""

import typing

from forml.flow.pipeline import topology
from forml.testing import spec, routine


def operator(subject: typing.Type[topology.Operator]) -> typing.Type[spec.Suite]:
    """Operator base class generator.

    Args:
        subject: Operator to be tested within given suite.
    """
    class Operator(spec.Suite, metaclass=spec.Meta):
        """Generated base class.
        """
        @property
        def __operator__(self) -> typing.Type[topology.Operator]:
            """Attached operator.

            Returns: Operator instance.
            """
            return subject
    return Operator
