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
import abc
import collections
import typing

import numpy

from forml.io import dsl

Array = typing.Sequence[typing.Any]  # Sequence of items (n-dimensional but only the top one need to be accessible)
ColumnMajor = Array  # Sequence of columns of any type (columnar, column-wise semantic)
RowMajor = Array  # Sequence of rows of any type (row-wise semantic)
Native = typing.TypeVar('Native')
Encoding = str  # Media type encoding


class Tabular:
    """Dataset interface providing both row/column oriented representation of the underlying data."""

    @abc.abstractmethod
    def to_columns(self) -> ColumnMajor:
        """Get the dataset in a column oriented structure.

        Returns:
            Column-wise dataset representation.
        """

    @abc.abstractmethod
    def to_rows(self) -> RowMajor:
        """Get the dataset in a row oriented structure.

        Returns:
            Row-wise dataset representation.
        """

    @abc.abstractmethod
    def take_rows(self, indices: typing.Sequence[int]) -> 'Tabular':
        """Slice the table returning new instance with just the selected rows.

        Args:
            indices: Column indices to take.

        Returns:
            New Tabular instance with just the given rows taken.
        """

    @abc.abstractmethod
    def take_columns(self, indices: typing.Sequence[int]) -> 'Tabular':
        """Slice the table returning new instance with just the selected columns.

        Args:
            indices: Column indices to take.

        Returns:
            New Tabular instance with just the given columns taken.
        """


class Dense(Tabular):
    """Simple Tabular implementation backed by numpy array."""

    def __init__(self, rows: numpy.ndarray):
        self._rows: numpy.ndarray = rows

    @staticmethod
    def _to_ndarray(data: Array) -> numpy.ndarray:
        """Helper for creating a ndarray instance.

        Args:
            data: Input array.

        Returns:
            NDArray instance.
        """
        return data if isinstance(data, numpy.ndarray) else numpy.array(data, dtype=object)

    @classmethod
    def from_columns(cls, columns: ColumnMajor) -> 'Dense':
        """Helper for creating Tabular from sequence of columns.

        Args:
            columns: Sequence of columns to use.

        Returns:
            Dense instance representing the columnar data.
        """
        return cls(cls._to_ndarray(columns).T)

    @classmethod
    def from_rows(cls, rows: RowMajor) -> 'Dense':
        """Helper for creating Tabular from sequence of rows.

        Args:
            rows: Sequence of rows to use.

        Returns:
            Dense instance representing the row data.
        """
        return cls(cls._to_ndarray(rows))

    def to_columns(self) -> ColumnMajor:
        return self._rows.T

    def to_rows(self) -> RowMajor:
        return self._rows

    def take_rows(self, indices: typing.Sequence[int]) -> 'Dense':
        return self.from_rows(self._rows.take(indices, axis=0))

    def take_columns(self, indices: typing.Sequence[int]) -> 'Dense':
        return self.from_columns(self._rows.T.take(indices, axis=0))


Entry = tuple[dsl.Schema, Tabular]
"""Product level input type."""
Result = tuple[dsl.Schema, RowMajor]
"""Product level output type."""


class Request(collections.namedtuple('Request', 'payload, encoding, accept')):
    """Application level request object."""

    payload: bytes
    """Encoded payload."""

    encoding: Encoding
    """Encoding media type."""

    accept: tuple[Encoding]
    """Accepted response media type."""

    def __new__(cls, payload: bytes, encoding: Encoding, accept: typing.Optional[typing.Sequence[Encoding]] = None):
        return super().__new__(cls, payload, encoding, tuple(accept or [encoding]))


class Response(typing.NamedTuple):
    """Application level response object."""

    payload: bytes
    """Encoded payload."""

    encoding: Encoding
    """Encoding media type."""
