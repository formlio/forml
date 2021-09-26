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
import inspect
import logging
import os
import typing

import setuptools
from setuptools import dist

from .. import _product
from .command import bdist, launch, upload

LOGGER = logging.getLogger(__name__)


class Distribution(dist.Distribution):  # pylint: disable=function-redefined
    """Extended distribution type with extra forml attributes."""

    COMMANDS: typing.Mapping[str, type[setuptools.Command]] = {
        'train': launch.Train,
        'tune': launch.Tune,
        'eval': launch.Eval,
        bdist.Package.COMMAND: bdist.Package,
        'upload': upload.Registry,
    }

    def __init__(self, attrs=None):
        # mapping between standard forml components and their implementing modules within the project
        self.component: typing.Mapping[str, str] = {}
        attrs = dict(attrs or ())
        attrs.setdefault('cmdclass', {}).update(self.COMMANDS)
        super().__init__(attrs)

    def run_commands(self):
        """Overriding the default functionality to allow bypassing the execution if not called from setup.py:setup.

        This is to avoid fork looping ie when using Dask multiprocessing runner.
        """
        if inspect.currentframe().f_back.f_back.f_back.f_globals.get('__name__') != '__main__':
            return
        super().run_commands()

    @property
    def artifact(self) -> _product.Artifact:
        """Get the artifact for this project.

        Returns:
            Artifact instance.
        """
        modules = dict(self.component)
        package = modules.pop('', None)
        if not package:
            for mod in modules.values():
                if '.' in mod:
                    package, _ = os.path.splitext(mod)
                    break
            else:
                package = self.packages[0]
        pkgdir = self.package_dir or {'': '.'}
        return _product.Artifact(pkgdir[''], package=package, **modules)
