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
Date and time manipulation functions.

.. spelling:word-list:: Datetime
"""
import operator

from .._struct import kind as kindmod
from .._struct import series


class Year(series.Univariate):
    """Extract the year from given date/time instance.

    Args:
        value: Date/time feature to extract the *year* value from.

    Raises:
         dsl.GrammarError: If ``value`` is not a valid date/time.

    Examples:
        >>> ETL = Student.select(function.Year(Student.birthday))
    """

    value: series.Operable = property(operator.itemgetter(0))
    kind: kindmod.Any = kindmod.Integer()

    def __new__(cls, value: series.Operable):
        kindmod.Date.ensure(series.Operable.ensure_is(value).kind)
        return super().__new__(cls, value)
