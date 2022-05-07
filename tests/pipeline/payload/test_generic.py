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
Generic payload operators unit tests.
"""
# pylint: disable=no-self-use

import pandas

from forml import testing
from forml.pipeline import payload


class TestMapReduce(testing.operator(payload.MapReduce)):
    """MapReduce operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})
    LABELS = pandas.Series([0, 1, 0], name='baz')

    apply_mode = (
        testing.Case(payload.SelectPandas.spec(columns=['foo']), payload.DropPandas.spec(columns=['foo']))
        .apply(FEATURES)
        .returns(FEATURES, testing.pandas_equals)
    )