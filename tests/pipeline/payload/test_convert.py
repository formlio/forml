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
Conversion unit tests.
"""
# pylint: disable=no-self-use
import numpy
import pandas

from forml import testing
from forml.pipeline import payload


class TestToPandas(testing.operator(payload.ToPandas)):  # pylint: disable=invalid-name
    """Conversion to_pandas operator unit tests."""

    EXPECTED_SERIES = pandas.Series([0, 1, 0], name='baz')
    EXPECTED_DATAFRAME = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})

    apply_dataframe = testing.Case().apply(EXPECTED_DATAFRAME).returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    apply_series = testing.Case().apply(EXPECTED_SERIES).returns(EXPECTED_SERIES, testing.pandas_equals)
    apply_numpy_array = (
        testing.Case(columns=('foo', 'bar'))
        .apply(numpy.array([[1.0, 'a'], [2.0, 'b'], [3.0, 'b']], dtype=object))
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    apply_numpy_vector = testing.Case().apply(numpy.array([0, 1, 0])).returns(EXPECTED_SERIES, testing.pandas_equals)
    apply_pylist_table = (
        testing.Case(columns=('foo', 'bar'))
        .apply([[1.0, 'a'], [2.0, 'b'], [3.0, 'b']])
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    apply_pylist_vector = testing.Case().apply([0, 1, 0]).returns(EXPECTED_SERIES, testing.pandas_equals)
    apply_pytuple_table = (
        testing.Case(columns=('foo', 'bar'))
        .apply(((1.0, 'a'), (2.0, 'b'), (3.0, 'b')))
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    apply_pytuple_vector = testing.Case().apply((0, 1, 0)).returns(EXPECTED_SERIES, testing.pandas_equals)
    train_dataframe = (
        testing.Case().train(EXPECTED_DATAFRAME, EXPECTED_SERIES).returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    train_series = (
        testing.Case().train(EXPECTED_SERIES, EXPECTED_SERIES).returns(EXPECTED_SERIES, testing.pandas_equals)
    )
    train_numpy_array = (
        testing.Case(columns=('foo', 'bar'))
        .train(numpy.array([[1.0, 'a'], [2.0, 'b'], [3.0, 'b']], dtype=object), numpy.array([0, 1, 0]))
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    train_numpy_vector = (
        testing.Case()
        .train(numpy.array([0, 1, 0]), numpy.array([0, 1, 0]))
        .returns(EXPECTED_SERIES, testing.pandas_equals)
    )
    train_pylist_table = (
        testing.Case(columns=('foo', 'bar'))
        .train([[1.0, 'a'], [2.0, 'b'], [3.0, 'b']], [0, 1, 0])
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    train_pylist_vector = testing.Case().train([0, 1, 0], [0, 1, 0]).returns(EXPECTED_SERIES, testing.pandas_equals)
    train_pytuple_table = (
        testing.Case(columns=('foo', 'bar'))
        .train(((1.0, 'a'), (2.0, 'b'), (3.0, 'b')), (0, 1, 0))
        .returns(EXPECTED_DATAFRAME, testing.pandas_equals)
    )
    train_pytuple_vector = testing.Case().train((0, 1, 0), (0, 1, 0)).returns(EXPECTED_SERIES, testing.pandas_equals)
