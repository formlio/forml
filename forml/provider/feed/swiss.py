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
Swiss army knife-like feed implementation.
"""
import abc
import pathlib
import typing

import pandas

from forml.io import dsl, layout
from forml.provider.feed import lazy


class Origin(lazy.Origin[None], metaclass=abc.ABCMeta):
    """Base class for data origin handlers."""

    def __init__(self, source: typing.Union[dsl.Source, str]):
        if isinstance(source, str):
            source = dsl.Source.from_path(source)
        self._source: dsl.Source = source

    @property
    def source(self) -> dsl.Source:
        """The DSL source description of this origin.

        The left side of the content resolving mapping (e.g. a ``dsl.Table`` schema).

        Returns:
            DSL representation of this origin.
        """
        return self._source

    @property
    def names(self) -> typing.Sequence[str]:
        """List of column names within this origin.

        Returns:
            Column names.
        """
        return [f.name for f in self.source.schema]


class Data(Origin):
    """Static data origin."""

    def __init__(self, schema: typing.Union[dsl.Source, str], content: layout.RowMajor):
        super().__init__(schema)
        self._content: pandas.DataFrame = pandas.DataFrame(content, columns=self.names)

    def load(self, partition: typing.Optional[None]) -> pandas.DataFrame:
        return self._content


class Csv(Origin):
    """CSV file origin."""

    OPTIONS = {'parse_dates': True, 'header': 0}

    def __init__(self, schema: typing.Union[dsl.Source, str], path: typing.Union[pathlib.Path, str], **kwargs):
        super().__init__(schema)
        self._path: typing.Union[pathlib.Path, str] = path
        self._kwargs = self.OPTIONS | {'names': self.names} | kwargs

    def load(self, partition: typing.Optional[None]) -> pandas.DataFrame:
        return pandas.read_csv(self._path, **self._kwargs)


class Feed(lazy.Feed, alias='swiss'):
    """Feed with multi-origin content."""

    def __init__(
        self,
        data: typing.Optional[typing.Mapping[typing.Union[dsl.Source, str], layout.ColumnMajor]] = None,
        csv: typing.Optional[
            typing.Mapping[
                typing.Union[dsl.Source, str],
                typing.Union[
                    pathlib.Path, str, tuple[typing.Union[pathlib.Path, str], typing.Mapping[str, typing.Any]]
                ],
            ]
        ] = None,
    ):
        origins = []
        if data:
            origins.extend(Data(t, c) for t, c in data.items())
        if csv:
            for schema, spec in csv.items():
                if isinstance(spec, (pathlib.Path, str)):
                    path, kwargs = spec, {}
                else:
                    path, kwargs = spec
                origins.append(Csv(schema, path, **kwargs))
        super().__init__(*origins)
