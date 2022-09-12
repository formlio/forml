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
Mathematical functions and operators.

Arithmetic Operators
--------------------

The following native arithmetic operators are available directly on any of the
:class:`dsl.Operable <forml.io.dsl.Operable>` instances:

============ =================
  Operator     Description
============ =================
  ``+``       Addition
  ``-``       Subtraction
  ``*``       Multiplication
  ``/``       Division
  ``%``       Modulus
============ =================

Examples:
    >>> ETL = Student.select(Student.surname, Student.score * Student.level * 0.32)


Mathematical Functions
----------------------
"""

from .._struct import kind as kindmod
from .._struct import series


class Abs(series.Arithmetic, series.Univariate):
    """Return the absolute value.

    Examples:
        >>> ETL = Student.select(Student.surname, function.Abs(Student.level - 5))
    """


class Ceil(series.Arithmetic, series.Univariate):
    """Return the value rounded up to the nearest integer.

    Examples:
        >>> ETL = Student.select(Student.surname, function.Ceil(Student.score))
    """

    kind: kindmod.Integer = kindmod.Integer()


class Floor(series.Arithmetic, series.Univariate):
    """Return the value rounded down to the nearest integer.

    Examples:
        >>> ETL = Student.select(Student.surname, function.Floor(Student.score))
    """

    kind: kindmod.Integer = kindmod.Integer()
