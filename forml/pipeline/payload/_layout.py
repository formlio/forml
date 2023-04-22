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
Payload layout implementations.
"""
import abc
import typing

import pandas

from forml.io import layout

if typing.TYPE_CHECKING:
    from forml.pipeline import payload


class Major(layout.Array, metaclass=abc.ABCMeta):
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


class Frame(layout.Tabular):
    """Simple Tabular implementation backed by :class:`pandas:pandas.DataFrame`."""

    class Rows(Major, layout.RowMajor):
        """Row oriented accessor."""

        @property
        def axis(self) -> int:
            return 0

    class Columns(Major, layout.ColumnMajor):
        """Column oriented accessor."""

        @property
        def axis(self) -> int:
            return 1

    def __init__(self, data: pandas.DataFrame):
        self._data: pandas.DataFrame = data

    def __eq__(self, other):
        return self._data.equals(other)

    def __hash__(self):
        return id(self._data)

    def to_columns(self) -> 'payload.Frame.Columns':
        return self.Columns(self._data)

    def to_rows(self) -> 'payload.Frame.Rows':
        return self.Rows(self._data)

    def take_rows(self, indices: typing.Sequence[int]) -> 'payload.Frame':
        return Frame(self._data.iloc[indices])

    def take_columns(self, indices: typing.Sequence[int]) -> 'payload.Frame':
        return Frame(self._data.iloc[:, indices])
