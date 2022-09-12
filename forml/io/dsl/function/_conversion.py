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
Conversion functions.
"""
import operator
import typing

from .._struct import kind as kindmod
from .._struct import series

if typing.TYPE_CHECKING:
    from forml.io import dsl


class Cast(series.Expression):
    """Explicitly cast value as the given :ref:`kind <dsl-kinds>`.

    Args:
        value: Value to be cast to the given type.
        kind: Type to cast to.

    Examples:
        >>> ETL = Student.select(function.Cast(Student.score, dsl.Integer()))
    """

    value: series.Operable = property(operator.itemgetter(0))
    kind: kindmod.Any = property(operator.itemgetter(1))

    def __new__(cls, value: 'dsl.Operable', kind: 'dsl.Any'):
        return super().__new__(cls, value, kind)
