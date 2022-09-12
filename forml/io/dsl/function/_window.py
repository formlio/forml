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
Functions that perform calculations across rows of the query result.

Todo:
    Support for window functions is experimental and unlikely to be supported by the existing
    parsers.

These function needs to be wrapped as a valid :class:`dsl.Feature <forml.io.dsl.Feature>` using the
:class:`dsl.Window <forml.io.dsl.Window>` API.


Aggregate Functions
-------------------

All :ref:`Aggregate functions <query-functions-aggregate>` can be used as window functions by
calling the :meth:`.over() <forml.io.dsl.Window.Function.over>` method.

Ranking Functions
-----------------
"""

from .._struct import kind as kindmod
from .._struct import series


class RowNumber(series.Window.Function):
    """A unique sequential number for each row starting from one according to the ordering of rows
    within the window partition.

    Examples:
        >>> ETL = Student.select(function.RowNumber().over(Student.surname))
    """

    kind: kindmod.Integer = kindmod.Integer()
