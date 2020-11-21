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
Payload utilities.
"""
import typing

Vector = typing.Sequence[typing.Any]  # Sequence of generic items
ColumnMajor = typing.Sequence[typing.Any]  # Sequence of columns of any type (columnar, column-wise semantic)
RowMajor = typing.Sequence[typing.Any]  # Sequence of rows of any type (row-wise semantic)
Native = typing.TypeVar('Native')


def transpose(data: typing.Sequence[Vector]) -> typing.Sequence[Vector]:
    """Primitive helper for transposing between row and column oriented generic matrices.

    Note this performs badly compared to implementations available on specific data formats like numpy ndarray.

    Args:
        data: Input matrix.

    Returns:
        Transposed output matrix.
    """

    def col(idx: int) -> Vector:
        """Create a vector for given column index.

        Args:
            idx: Index of column to be generated.

        Returns:
            Vector for given column.
        """
        return [data[r][idx] for r in range(nrows)]

    if data:
        nrows = len(data)
        ncols = len(data[0])
        data = [col(c) for c in range(ncols)]
    return data
