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
Convenience testing matcher implementations.
"""
import sys

from pandas.core import generic as pdtype


def pandas_equals(expected: pdtype.NDFrame, actual: pdtype.NDFrame) -> bool:
    """Compare Pandas DataFrames for equality.

    Args:
        expected: Instance of the expected data representation.
        actual: Testcase produced data.

    Returns:
        True if the data is equal.
    """
    if not hasattr(actual, 'equals') or not actual.equals(expected):
        print(f'Data mismatch: {expected} vs {actual}', file=sys.stderr)
        return False
    return True
