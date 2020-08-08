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
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root as rootmod, project as prjmod, lineage as lngmod


class TestRegistry:
    """Registry unit tests.
    """

    def test_get(self, registry: persistent.Registry, project_name: prjmod.Level.Key,
                 populated_lineage: lngmod.Level.Key):
        """Test lineage get.
        """
        lineage = rootmod.Level(registry).get(project_name).get(populated_lineage)
        assert lineage.key == populated_lineage
