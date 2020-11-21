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
Static feed implementation.
"""
import types
import typing

from forml.io import feed, payload
from forml.io.dsl import error
from forml.io.dsl.struct import frame, series


class Feed(feed.Provider[None, payload.Vector]):
    """Static feed is initialized with actual data which can only be returned in primitive column-wise fashion. No
    advanced ETL can be applied.
    """

    def __init__(self, data: typing.Mapping[frame.Table, payload.ColumnMajor]):
        super().__init__()
        self._sources: typing.Mapping[frame.Source, None] = types.MappingProxyType({f: None for f in data})
        self._columns: typing.Mapping[series.Column, payload.Vector] = types.MappingProxyType(
            {c: s for t, f in data.items() for c, s in zip(t.columns, f)}
        )

    #  pylint: disable=unused-argument
    @classmethod
    def reader(
        cls,
        sources: typing.Mapping[frame.Source, None],
        columns: typing.Mapping[series.Column, payload.Vector],
        **kwargs: typing.Any,
    ) -> typing.Callable[[frame.Query], payload.ColumnMajor]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader)."""

        def read(query: frame.Query) -> payload.ColumnMajor:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns:
                Data.
            """
            if query.prefilter or query.postfilter or query.ordering or query.rows:
                raise error.Unsupported('Query not supported by static feed')
            try:
                return [columns[c] for c in query.columns]
            except KeyError as err:
                raise error.Unsupported(f'Column not supported by static feed: {err}')

        return read

    @property
    def sources(self) -> typing.Mapping[frame.Source, None]:
        """The explicit sources mapping implemented by this feed to be used by the query parser."""
        return self._sources

    @property
    def columns(self) -> typing.Mapping[series.Column, payload.Vector]:
        """The explicit columns mapping implemented by this feed to be used by the query parser."""
        return self._columns
