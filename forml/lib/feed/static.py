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

from forml import io
from forml.io import dsl, layout


class Feed(io.Feed[None, layout.Vector]):
    """Static feed is initialized with actual data which can only be returned in primitive column-wise fashion. No
    advanced ETL can be applied.
    """

    def __init__(self, data: typing.Mapping[dsl.Table, layout.ColumnMajor]):
        super().__init__()
        self._sources: typing.Mapping[dsl.Source, None] = types.MappingProxyType({f: None for f in data})
        self._features: typing.Mapping[dsl.Feature, layout.Vector] = types.MappingProxyType(
            {c: s for t, f in data.items() for c, s in zip(t.features, f)}
        )

    #  pylint: disable=unused-argument
    @classmethod
    def reader(
        cls,
        sources: typing.Mapping[dsl.Source, None],
        features: typing.Mapping[dsl.Feature, layout.Vector],
        **kwargs: typing.Any,
    ) -> typing.Callable[[dsl.Query], layout.ColumnMajor]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader)."""

        def read(query: dsl.Query) -> layout.ColumnMajor:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns:
                Data.
            """
            if query.prefilter or query.postfilter or query.ordering or query.rows:
                raise dsl.UnsupportedError('Query not supported by static feed')
            try:
                return tuple(features[c] for c in query.features)
            except KeyError as err:
                raise dsl.UnsupportedError(f'Column not supported by static feed: {err}')

        return read

    @property
    def sources(self) -> typing.Mapping[dsl.Source, None]:
        """The explicit sources mapping implemented by this feed to be used by the query parser."""
        return self._sources

    @property
    def features(self) -> typing.Mapping[dsl.Feature, layout.Vector]:
        """The explicit features mapping implemented by this feed to be used by the query parser."""
        return self._features
