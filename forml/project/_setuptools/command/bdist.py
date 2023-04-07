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
Custom setuptools commands distribution packaging.
"""
import os
import pathlib
import shutil
import subprocess
import sys
import typing

import setuptools

from ... import _distribution


class Package(setuptools.Command):
    """ForML build package."""

    COMMAND = f'bdist_{_distribution.Package.FORMAT}'
    description = 'create a ForML distribution'

    user_options = [
        ('bdist-dir=', 'b', 'temporary directory for creating the distribution'),
        ('dist-dir=', 'd', 'directory to put final built distributions in'),
    ]

    def initialize_options(self) -> None:
        """Init options."""
        self.bdist_dir: typing.Optional[str] = None
        self.dist_dir: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options."""
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base, _distribution.Package.FORMAT)

        need_options = ('dist_dir',)
        self.set_undefined_options('bdist', *zip(need_options, need_options))

    @property
    def filename(self) -> str:
        """Target package file name."""
        return f'{self.distribution.tree.name}-{self.distribution.tree.version}.{_distribution.Package.FORMAT}'

    @property
    def manifest(self) -> _distribution.Manifest:
        """Package manifest."""
        return _distribution.Manifest(
            name=self.distribution.tree.name,
            version=self.distribution.tree.version,
            package=self.distribution.tree.package,
            **self.distribution.tree.components,
        )

    def run(self) -> None:
        """Trigger the packaging process."""
        if os.path.exists(self.bdist_dir):
            shutil.rmtree(self.bdist_dir)
        subprocess.run(
            [
                sys.executable,
                '-m',
                'pip',
                'install',
                '--no-user',
                '--disable-pip-version-check',
                '--exists-action=a',
                '--no-warn-conflicts',
                '--no-compile',
                '--target',
                self.bdist_dir,
                *self.distribution.tree.dependencies,
            ],
            check=True,
        )
        package = self.distribution.tree.package.rsplit('.', 1)[0]
        shutil.copytree(self.distribution.tree.path / package, pathlib.Path(self.bdist_dir) / package)
        if not os.path.exists(self.dist_dir):
            os.makedirs(self.dist_dir)
        target = os.path.join(self.dist_dir, self.filename)
        data = (self.COMMAND, '', str(_distribution.Package.create(self.bdist_dir, self.manifest, target).path))
        if data not in self.distribution.dist_files:
            self.distribution.dist_files.append(data)
