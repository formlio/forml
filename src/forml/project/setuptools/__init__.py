# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Customized setuptools.
"""
import logging
import typing

import setuptools
# this makes ourselves a drop-in replacement of original setuptools
from setuptools import *  # pylint: disable=redefined-builtin; # noqa: F401,F402,F403
from setuptools import dist

from forml.project.setuptools.command import launch, bdist, upload

LOGGER = logging.getLogger(__name__)


class Distribution(dist.Distribution):  # pylint: disable=function-redefined
    """Extended distribution type with extra forml attributes.
    """
    def __init__(self, attrs=None):
        # mapping between standard forml components and their implementing modules within the project
        self.component: typing.Mapping[str, str] = dict()
        super().__init__(attrs)


COMMANDS: typing.Mapping[str, typing.Type[launch.Mode]] = {
    'train': launch.Train,
    'tune': launch.Tune,
    'score': launch.Score,
    bdist.Package.COMMAND: bdist.Package,
    'upload': upload.Registry
}

OPTIONS = {
    'distclass': Distribution,
    'cmdclass': COMMANDS,
}


def setup(**kwargs) -> dist.Distribution:  # pylint: disable=function-redefined
    """Setuptools wrapper for defining user projects using setup.py.

    Args:
        **kwargs: Standard setuptools keyword arguments.

    Returns: setuptools distribution object.
    """
    return setuptools.setup(**{**kwargs, **OPTIONS})
