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
import cgi
import collections
import fnmatch
import functools
import re
import types
import typing

import numpy

from forml.io import dsl

Array = typing.Sequence[typing.Any]  # Sequence of items (n-dimensional but only the top one need to be accessible)
ColumnMajor = Array  # Sequence of columns of any type (columnar, column-wise semantic)
RowMajor = Array  # Sequence of rows of any type (row-wise semantic)
Native = typing.TypeVar('Native')


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

    def __eq__(self, other):
        return isinstance(other, self.__class__) and numpy.array_equal(self._rows, other._rows)

    def __hash__(self):
        return hash(self._rows)

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


class Entry(typing.NamedTuple):
    """Product level input type."""

    schema: dsl.Source.Schema
    data: Tabular


class Outcome(typing.NamedTuple):
    """Product level output type."""

    schema: dsl.Source.Schema
    data: RowMajor


_CSV = re.compile(r'\s*,\s*')


class Encoding(collections.namedtuple('Encoding', 'kind, options')):
    """Content encoding representation."""

    kind: str
    """Content type label."""
    options: typing.Mapping[str, str]
    """Encoding options."""

    def __new__(cls, kind: str, **options: str):
        return super().__new__(cls, kind.strip().lower(), types.MappingProxyType(options))

    @functools.cached_property
    def header(self) -> str:
        """Get the header-formatted representation of this encoding.

        Returns:
            Header formatted representation.
        """
        value = self.kind
        if self.options:
            value += '; ' + '; '.join(f'{k}={v}' for k, v in self.options.items())
        return value

    @classmethod
    @functools.lru_cache(256)
    def parse(cls, value: str) -> typing.Sequence['Encoding']:
        """Parse the mime header value.

        Args:
            value: Comma separated list of mime values and their parameters

        Returns:
            Sequence of the mime values ordered according to the provided priority.
        """
        return tuple(
            cls(m, **{k: v for k, v in o.items() if k != 'q'})
            for m, o in sorted(
                (cgi.parse_header(h) for h in _CSV.split(value)),
                key=lambda t: float(t[1].get('q', 1)),
                reverse=True,
            )
        )

    @functools.cache
    def match(self, other: 'Encoding') -> bool:
        """Return ture if the other encoding matches ours including glob wildcards.

        Args:
            other: Encoding to match against this - must not contain wildcards.

        Returns:
            True if matches.
        """
        return (
            '*' not in other.kind
            and fnmatch.fnmatch(other.kind, self.kind)
            and all(other.options.get(k) == v for k, v in self.options.items())
        )

    def __hash__(self):
        return hash(self.kind) ^ hash(tuple(sorted(self.options.items())))

    def __str__(self):
        return self.kind

    def __getnewargs_ex__(self):
        return (self.kind,), dict(self.options)


class Request(collections.namedtuple('Request', 'payload, encoding, params, accept')):
    """Application level request object."""

    class Decoded(typing.NamedTuple):
        """Decoded request case class."""

        entry: Entry
        """Input data."""

        scope: typing.Any = None
        """Custom (serializable!) metadata produced by (user-defined) decoder and carried through the system."""

    payload: bytes
    """Encoded payload."""

    encoding: Encoding
    """Encoding media type."""

    params: typing.Mapping[str, typing.Any]
    """Optional application-level parameters."""

    accept: tuple[Encoding]
    """Accepted response media type."""

    def __new__(
        cls,
        payload: bytes,
        encoding: Encoding,
        params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        accept: typing.Optional[typing.Sequence[Encoding]] = None,
    ):
        return super().__new__(
            cls, payload, encoding, types.MappingProxyType(dict(params or {})), tuple(accept or [encoding])
        )

    def __getnewargs__(self):
        return self.payload, self.encoding, dict(self.params), self.accept


class Response(typing.NamedTuple):
    """Application level response object."""

    payload: bytes
    """Encoded payload."""

    encoding: Encoding
    """Encoding media type."""


class Stats(typing.NamedTuple):
    """Application specific serving metrics."""
