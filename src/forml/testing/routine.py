import logging

import typing

from forml.flow.pipeline import topology
from forml.testing import spec

LOGGER = logging.getLogger(__name__)


def case(title: str, operator: typing.Type[topology.Operator], scenario: 'spec.Scenario') -> None:
    LOGGER.debug('Testing %s[%s]', operator, title)
