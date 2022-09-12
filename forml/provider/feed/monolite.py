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
Special feed allowing to combine multiple simple sources.
"""
import abc
import pathlib
import typing

import pandas

import forml
from forml.io import dsl
from forml.provider.feed import lazy

if typing.TYPE_CHECKING:
    from forml.io import layout


class Origin(lazy.Origin[None], metaclass=abc.ABCMeta):
    """Base class for data origin handlers."""

    def __init__(self, source: typing.Union['dsl.Source', str]):
        if isinstance(source, str):
            source = dsl.Source.from_path(source)
        self._source: 'dsl.Source' = source

    @classmethod
    def parse_config(cls, config: typing.Any) -> typing.Mapping[str, typing.Any]:  # pylint: disable=unused-argument
        """Parse the configuration options into the init ``**kwargs``.

        Args:
            config: Raw config options.

        Returns:
            Init ``**kwargs``.

        Raises:
            forml.InvalidError: In case of invalid configuration options.
        """
        return {}

    @classmethod
    def create(cls, setup: typing.Mapping[typing.Union['dsl.Source', str], typing.Any]) -> typing.Sequence['Origin']:
        """Factory method for creating origin instances out of the setup mapping.

        Args:
            setup: Mapping of DSL schema instances or fully qualified string paths to the origin
                   configuration parameters.

        Returns:
            Sequence of the parsed origin instances.
        """
        return tuple(cls(s, **cls.parse_config(o)) for s, o in setup.items())

    @property
    def source(self) -> 'dsl.Source':
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


class Inline(Origin):
    """Inline data origin."""

    def __init__(self, schema: typing.Union['dsl.Source', str], content: 'layout.RowMajor'):
        super().__init__(schema)
        self._content: pandas.DataFrame = pandas.DataFrame(content, columns=self.names)

    @classmethod
    def parse_config(cls, config: typing.Any) -> typing.Mapping[str, typing.Any]:
        return {'content': config}

    def load(self, partition: typing.Optional[None]) -> pandas.DataFrame:
        return self._content


class Csv(Origin):
    """CSV file origin."""

    OPTIONS = {'parse_dates': True, 'header': 0}

    def __init__(self, schema: typing.Union['dsl.Source', str], path: typing.Union[pathlib.Path, str], **kwargs):
        super().__init__(schema)
        self._path: typing.Union[pathlib.Path, str] = path
        self._kwargs = self.OPTIONS | {'names': self.names} | kwargs

    @classmethod
    def parse_config(
        cls, config: typing.Union[pathlib.Path, str, typing.Mapping[str, typing.Any]]
    ) -> typing.Mapping[str, typing.Any]:
        if isinstance(config, typing.Mapping):
            try:
                return config.get('kwargs', {}) | {'path': config['path']}
            except KeyError as err:
                raise forml.MissingError('Missing required parameter `path`') from err
        else:
            return {'path': config}

    def load(self, partition: typing.Optional[None]) -> pandas.DataFrame:
        return pandas.read_csv(self._path, **self._kwargs)


class Feed(lazy.Feed, alias='monolite'):
    """Lightweight feed for pulling data from multiple simple origins.

    The feed can resolve queries across all of its combined data sources.

    All the origins need to be declared using a proper :ref:`content resolver <io-resolution>`
    mapping with keys representing the fully qualified schema name formatted as
    ``<full.module.path>:<qualified.Class.Name>`` and the values should be origin-specific
    configuration options.

    Attention:
        All the referenced :ref:`schema catalogs <io-catalog>` must be installed.

    Supported origins:

    * *Inline* data provided as a row-oriented array.
    * *CSV files* parsed using the :func:`pandas:pandas.read_csv`.

    Args:
        inline: Schema mapping of datasets provided inline as native row-oriented arrays.
        csv: Schema mapping of datasets accessible using a CSV reader. Values can either be
             direct file system paths or mapping with two keys:

             * ``path`` pointing to the CSV file
             * ``kwargs`` containing additional options to be passed to the underlying
               :func:`pandas:pandas.read_csv`

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [FEED.mono]
        provider = "monolite"
        [FEED.mono.inline]
        "foobar.schemas:Foo.Baz" = [
            ["alpha", 27, 0.314, 2021-05-11T17:12:24],
            ["beta", 11, -1.12, 2020-11-03T01:24:56],
        ]
        [FEED.mono.csv]
        "openschema.kaggle:Titanic" = "/tmp/titanic.csv"
        [FEED.mono.csv."openschema.sklearn:Iris"]
        path = "/tmp/iris.csv"
        kwargs = {sep = ";", engine = "pyarrow"}

    Important:
        Select the ``sql`` :ref:`extras to install <install-extras>` ForML together with the
        SQLAlchemy support.

    Todo:
        * More file types (json, parquet)
        * Multi-file data sources (partitions)
    """

    def __init__(
        self,
        inline: typing.Optional[typing.Mapping[typing.Union['dsl.Source', str], 'layout.RowMajor']] = None,
        csv: typing.Optional[
            typing.Mapping[
                typing.Union['dsl.Source', str],
                typing.Union[pathlib.Path, str, typing.Mapping[str, typing.Any]],
            ]
        ] = None,
    ):
        origins = []
        if inline:
            origins.extend(Inline.create(inline))
        if csv:
            origins.extend(Csv.create(csv))
        super().__init__(*origins)
