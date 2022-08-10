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
import pandas

from forml import testing
from forml.pipeline import payload, wrap


class TestMapReduce(testing.operator(payload.MapReduce)):
    """MapReduce operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})
    LABELS = pandas.Series([0, 1, 0], name='baz')

    apply_mode = (
        testing.Case(payload.PandasSelect.builder(columns=['foo']), payload.PandasDrop.builder(columns=['foo']))
        .apply(FEATURES)
        .returns(FEATURES, testing.pandas_equals)
    )


class TestApply(testing.operator(wrap.Operator.mapper(payload.Apply))):
    """Apply actor unit tests."""

    apply = testing.Case(function=lambda i: i + 1).apply(10).returns(11)


class TestPandasSelect(testing.operator(wrap.Operator.mapper(payload.PandasSelect))):
    """PandasSelect operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})

    select = testing.Case(columns=['foo']).apply(FEATURES).returns(FEATURES[['foo']], testing.pandas_equals)


class TestPandasDrop(testing.operator(wrap.Operator.mapper(payload.PandasDrop))):
    """PandasDrop operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})

    drop = testing.Case(columns=['foo']).apply(FEATURES).returns(FEATURES.drop(columns=['foo']), testing.pandas_equals)
