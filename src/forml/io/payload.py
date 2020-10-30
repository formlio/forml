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

Columnar = typing.Sequence[typing.Any]  # Sequence of columns of any type
Native = typing.TypeVar('Native')


def transpose(data: typing.Sequence[typing.Sequence[typing.Any]]) -> typing.Sequence[typing.Iterator[typing.Any]]:
    """Helper for transposing between row and column oriented matrices.

    Args:
        data: Input matrix.

    Returns: Transposed output matrix.
    """
    def col(idx: int) -> typing.Iterator[typing.Any]:
        """Create a vector for given column index.

        Args:
            idx: Index of column to be generated.

        Returns: Vector for given column.
        """
        return [data[r][idx] for r in range(nrows)]

    if data:
        nrows = len(data)
        ncols = len(data[0])
        data = [col(c) for c in range(ncols)]
    return data
