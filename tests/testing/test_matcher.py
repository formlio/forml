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
ForML testing matchers unit tests.
"""
# pylint: disable=protected-access

import pandas
import pytest

from forml import testing


@pytest.mark.parametrize(
    'expected, actual, result',
    [
        (1, 1, False),
        (pandas.DataFrame({'a': [1, 2]}), pandas.DataFrame({'a': [1, 2]}), True),
        (pandas.DataFrame({'a': [2, 1]}), pandas.DataFrame({'a': [1, 2]}), False),
        (pandas.DataFrame({'B': [1, 2]}), pandas.DataFrame({'a': [1, 2]}), False),
        (pandas.Series([1, 2]), pandas.Series([1, 2]), True),
        (pandas.Series([2, 1]), pandas.Series([1, 2]), False),
    ],
)
def test_pandas_equals(expected, actual, result: bool):
    """NDFrame equality test."""
    assert testing.pandas_equals(expected, actual) == result
