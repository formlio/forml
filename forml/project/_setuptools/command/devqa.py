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
Custom setuptools commands for unit testing.
"""
import abc
import operator

from setuptools.command import test


class Test(test.test, metaclass=abc.ABCMeta):
    """Command to run unit tests after in-place build"""

    description = 'run unit tests after in-place build'

    def run(self) -> None:
        """This is the original test command entry point - let's override it with our actions."""

        installed_dists = self.install_dists(self.distribution)

        cmd = ' '.join(self._argv)
        if self.dry_run:
            self.announce(f'skipping "{cmd}" (dry run)')
            return

        self.announce(f'running "{cmd}"')

        paths = map(operator.attrgetter('location'), installed_dists)
        with self.paths_on_pythonpath(paths):
            with self.project_on_sys_path():
                self.run_tests()
