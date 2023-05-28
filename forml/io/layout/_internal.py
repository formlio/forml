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
Internal payload utilities.
"""
import abc
import typing

import numpy
import pandas

if typing.TYPE_CHECKING:
    from forml.io import layout


#: Sequence of items (n-dimensional but only the top one needs to be accessible).
Array = typing.Sequence[typing.Any]
#: Sequence of columns of any type (columnar, column-wise semantic).
ColumnMajor = Array
#: Sequence of rows of any type (row-wise semantic).
RowMajor = Array
#: Generic type variable representing arbitrary native type.
Native = typing.TypeVar('Native')


class Tabular:
    """Dataset interface providing both *row* and *column*-oriented representation of the underlying
    data.

    This is a lightweight interface to be used internally for data payload as returned by the Feed
    ``Reader`` only to be immediately turned to ``RowMajor`` representation once leaving the Feed
    ``Slicer``.
    """

    def __eq__(self, other):
        return isinstance(other, Tabular) and all(
            s == o for i, j in zip(self.to_rows(), other.to_rows()) for s, o in zip(i, j)
        )

    @abc.abstractmethod
    def to_columns(self) -> 'layout.ColumnMajor':
        """Get the dataset in a column-oriented structure.

        Returns:
            Column-wise dataset representation.
        """

    @abc.abstractmethod
    def to_rows(self) -> 'layout.RowMajor':
        """Get the dataset in a row-oriented structure.

        Returns:
            Row-wise dataset representation.
        """

    @abc.abstractmethod
    def take_rows(self, indices: typing.Sequence[int]) -> 'layout.Tabular':
        """Slice the table returning a new instance with just the selected rows.

        Args:
            indices: Row indices to take.

        Returns:
            New instance with just the given rows taken.
        """

    @abc.abstractmethod
    def take_columns(self, indices: typing.Sequence[int]) -> 'layout.Tabular':
        """Slice the table returning a new instance with just the selected columns.

        Args:
            indices: Column indices to take.

        Returns:
            New instance with just the given columns taken.
        """


class Dense(Tabular):
    """Simple Tabular implementation backed by numpy array."""

    def __init__(self, rows: numpy.ndarray):
        self._rows: numpy.ndarray = rows

    def __eq__(self, other):
        return numpy.array_equal(self._rows, other._rows) if isinstance(other, Dense) else super().__eq__(other)

    def __hash__(self):
        return id(self._rows)

    @staticmethod
    def _to_ndarray(data: 'layout.Array') -> numpy.ndarray:
        """Helper for creating a ndarray instance.

        Args:
            data: Input array.

        Returns:
            NDArray instance.
        """
        return data if isinstance(data, numpy.ndarray) else numpy.array(data, dtype=object)

    @classmethod
    def from_columns(cls, columns: 'layout.ColumnMajor') -> 'layout.Dense':
        """Helper for creating Tabular from sequence of columns.

        Args:
            columns: Sequence of columns to use.

        Returns:
            Dense instance representing the columnar data.
        """
        return cls(cls._to_ndarray(columns).T)

    @classmethod
    def from_rows(cls, rows: 'layout.RowMajor') -> 'layout.Dense':
        """Helper for creating Tabular from sequence of rows.

        Args:
            rows: Sequence of rows to use.

        Returns:
            Dense instance representing the row data.
        """
        return cls(cls._to_ndarray(rows))

    def to_columns(self) -> 'layout.ColumnMajor':
        return self._rows.T

    def to_rows(self) -> 'layout.RowMajor':
        return self._rows

    def take_rows(self, indices: typing.Sequence[int]) -> 'layout.Dense':
        return self.from_rows(self._rows.take(indices, axis=0))

    def take_columns(self, indices: typing.Sequence[int]) -> 'layout.Dense':
        return self.from_columns(self._rows.T.take(indices, axis=0))


class Frame(Tabular):
    """Simple Tabular implementation backed by :class:`pandas:pandas.DataFrame`."""

    class Major(Array, metaclass=abc.ABCMeta):
        """Base class for Row/Column oriented slices."""

        def __init__(self, frame: pandas.DataFrame):
            self.frame: pandas.DataFrame = frame

        @property
        @abc.abstractmethod
        def axis(self) -> int:
            """The axis number along which this slicer operates."""

        def __getitem__(self, index):
            iloc = [index, index]
            axis = 1 - self.axis
            iloc[axis] = range(len(self.frame.axes[axis]))
            result = self.frame.iloc[tuple(iloc)]
            if isinstance(index, int):
                return result
            return self.__class__(result)

        def __len__(self) -> int:
            return len(self.frame.axes[self.axis])

    class Rows(Major, RowMajor):
        """Row oriented accessor."""

        @property
        def axis(self) -> int:
            return 0

    class Columns(Major, ColumnMajor):
        """Column oriented accessor."""

        @property
        def axis(self) -> int:
            return 1

    def __init__(self, data: pandas.DataFrame):
        self._data: pandas.DataFrame = data

    def __eq__(self, other):
        return self._data.equals(other) if isinstance(other, Frame) else super().__eq__(other)

    def __hash__(self):
        return id(self._data)

    def to_columns(self) -> 'layout.Frame.Columns':
        return self.Columns(self._data)

    def to_rows(self) -> 'layout.Frame.Rows':
        return self.Rows(self._data)

    def take_rows(self, indices: typing.Sequence[int]) -> 'layout.Frame':
        return Frame(self._data.iloc[list(indices)])

    def take_columns(self, indices: typing.Sequence[int]) -> 'layout.Frame':
        return Frame(self._data.iloc[:, list(indices)])
