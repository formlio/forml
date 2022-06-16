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
Project steuptools tests.
"""
import pytest
import setuptools

from forml import project


class TestDistribution:
    """Distribution unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def distribution(project_package: project.Package) -> project.Distribution:
        """Test project distribution fixture."""

        return setuptools.setup(
            name=str(project_package.manifest.name),
            version=str(project_package.manifest.version),
            packages=setuptools.find_namespace_packages(
                where=project_package.path, include=[f'{project_package.manifest.package}*']
            ),
            package_dir={'': project_package.path},
            distclass=project.Distribution,
            script_args=['--version'],
        )

    def test_artifact(self, distribution: project.Distribution, project_artifact: project.Artifact):
        """Test the artifact loaded using this distribution."""
        assert distribution.artifact == project_artifact
