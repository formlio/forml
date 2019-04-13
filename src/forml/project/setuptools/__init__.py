"""
Customized setuptools.
"""
import logging

import setuptools
from setuptools import *  # this makes ourselves a drop-in replacement of original setuptools
import typing
from setuptools import dist

from forml.project.setuptools import command

LOGGER = logging.getLogger(__name__)


class Distribution(dist.Distribution):
    """Extended distribution type with extra forml attributes.
    """
    def __init__(self, attrs=None):
        # mapping between standard forml components and their implementing modules within the project
        self.component: typing.Mapping[str, str] = dict()
        super().__init__(attrs)


COMMANDS: typing.Mapping[str, typing.Type[Command]] = {
    'train': command.Train,
}

OPTIONS = {
    'distclass': Distribution,
    'cmdclass': COMMANDS,
}


def setup(**kwargs) -> dist.Distribution:
    """Setuptools wrapper for defining user projects using setup.py.

    Args:
        **kwargs: Standard setuptools keyword arguments.

    Returns: setuptools distribution object.
    """
    return setuptools.setup(**{**kwargs, **OPTIONS})
