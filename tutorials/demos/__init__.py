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
# pylint: disable=no-value-for-parameter
"""
Forml demos - common setup.
"""

from forml import project
from forml.io import dsl
from forml.pipeline import payload
from forml.provider.feed import monolite


class Demo(dsl.Schema):
    """Demo dataset schema."""

    Ordinal = dsl.Field(dsl.Integer())
    Label = dsl.Field(dsl.Integer())
    Feature = dsl.Field(dsl.Integer())


#: Demo dataset.
DATA = [[3, 1, 10], [4, 0, 11], [5, 1, 12], [6, 0, 13], [7, 1, 14], [8, 0, 15]]

#: Demo Feed preloaded with the DATA represented by the Demo schema
FEED = monolite.Feed(inline={Demo: DATA})

#: Common Source component for all the demo pipelines
SOURCE = project.Source.query(Demo.select(Demo.Feature), Demo.Label, ordinal=Demo.Ordinal) >> payload.ToPandas(
    columns=['Feature']
)
