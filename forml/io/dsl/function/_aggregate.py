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
Aggregate functions that operate on a :meth:`group <forml.io.dsl.Queryable.groupby>` of features
to produce a single result.
"""

from .._struct import kind as kindmod
from .._struct import series


class Count(series.Aggregate, series.Univariate):
    """Number of the input rows returned by query.

    Examples:
        >>> ETL = (
        ...     Student
        ...     .select(Student.level, function.Count(Student.id))
        ...     .groupby(Student.level)
        ... )
    """

    kind: kindmod.Integer = kindmod.Integer()


class Avg(series.Arithmetic, series.Aggregate, series.Univariate):
    """Average of the feature values.

    Examples:
        >>> ETL = (
        ...     Student
        ...     .select(Student.level, function.Avg(Student.score))
        ...     .groupby(Student.level)
        ... )
    """


class Max(series.Arithmetic, series.Aggregate, series.Univariate):
    """Maximum of the feature values.

    Examples:
        >>> ETL = (
        ...     Student
        ...     .select(Student.level, function.Max(Student.score))
        ...     .groupby(Student.level)
        ... )
    """


class Min(series.Arithmetic, series.Aggregate, series.Univariate):
    """Minimum of the feature values.

    Examples:
        >>> ETL = (
        ...     Student
        ...     .select(Student.level, function.Min(Student.score))
        ...     .groupby(Student.level)
        ... )
    """


class Sum(series.Arithmetic, series.Aggregate, series.Univariate):
    """Sum of the feature values.

    Examples:
        >>> ETL = (
        ...     Run
        ...     .select(Run.month, function.Sum(Run.distance))
        ...     .groupby(Run.month)
        ... )
    """
