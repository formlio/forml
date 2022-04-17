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
Custom setuptools commands distribution publishing.
"""
import typing

import setuptools

import forml
from forml import runtime
from forml.conf.parsed import provider as provcfg

from ... import _distribution
from . import bdist


class Registry(setuptools.Command):
    """ForML publish package."""

    description = 'publish a ForML distribution'

    user_options = [
        ('registry=', 'P', 'persistent registry to deploy to'),
    ]

    def initialize_options(self) -> None:
        """Init options."""
        self.registry: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options."""

    def run(self) -> None:
        """Trigger the deployment process."""
        packages = [_distribution.Package(f) for c, _, f in self.distribution.dist_files if c == bdist.Package.COMMAND]
        if not packages:
            raise forml.InvalidError(
                'Must create and upload files in one command ' f'(e.g. setup.py {bdist.Package.COMMAND} upload)'
            )
        project = self.distribution.get_name()
        platform = runtime.Platform(registry=provcfg.Registry.resolve(self.registry))
        for pkg in packages:
            platform.registry.publish(project, pkg)
