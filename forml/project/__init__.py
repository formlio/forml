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
Project management mechanics.
"""
import pathlib
import typing

from ._body import Artifact, Components
from ._component import Evaluation, Source, setup
from ._distribution import Manifest, Package
from ._setuptools import Distribution

__all__ = [
    'open',
    'setup',
    'Source',
    'Evaluation',
    'Components',
    'Artifact',
    'Distribution',
    'Manifest',
    'Package',
]


def open(  # pylint: disable=redefined-builtin
    path: typing.Optional[typing.Union[str, pathlib.Path]] = None,
    package: typing.Optional[str] = None,
    **modules: typing.Any,
) -> Artifact:
    """Getting a programmatic handle to a local ForML project.

    Args:
        path: File system path to the project source package root.
        package: Project package name.
        modules: Project component module path mappings.

    Returns:
        Project artifact.
    """
    return Artifact(path, package, **modules)
