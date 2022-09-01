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

#: Demo dataset.
DATA = [[1, 10], [1, 11], [1, 12], [0, 13], [0, 14], [0, 15]]


class Demo(dsl.Schema):
    """Demo dataset schema."""

    Label = dsl.Field(dsl.Integer())
    Age = dsl.Field(dsl.Integer())


#: Demo Feed preloaded with the DATA represented by the Demo schema
FEED = monolite.Feed(inline={Demo: DATA})

#: Common Source component for all the demo pipelines
SOURCE = project.Source.query(Demo.select(Demo.Age), Demo.Label) >> payload.ToPandas(columns=['Age'])
