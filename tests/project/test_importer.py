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
Project importer tests.
"""
# pylint: disable=no-self-use

import pytest

from forml.project import importer, distribution


def test_isolated(project_package: distribution.Package):
    """Isolated importer unit test.
    """
    with pytest.raises(ModuleNotFoundError):
        importer.isolated(project_package.manifest.package)
    importer.isolated(project_package.manifest.package, project_package.path)
