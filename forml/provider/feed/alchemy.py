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
SQLAlchemy based feed implementation.
"""
import functools
import hashlib
import logging
import pathlib
import re
import types
import typing

import pandas
import sqlalchemy
from sqlalchemy import sql

import forml
from forml import io, setup
from forml.io import dsl as dslmod
from forml.provider.feed.reader.sql import alchemy

if typing.TYPE_CHECKING:
    from forml.io import dsl  # pylint: disable=reimported

LOGGER = logging.getLogger(__name__)


class Results:
    """Filesystem backed result cache."""

    def __init__(self, path: pathlib.Path):
        self._frames: dict[str, pandas.DataFrame] = {}
        self._path: pathlib.Path = path
        self._path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _statement2key(statement: sql.Selectable) -> str:
        """Get the key for the given statement.

        Args:
            statement: Query statement.

        Returns:
            Query key.
        """
        return hashlib.sha256(str(statement.compile(compile_kwargs={'literal_binds': True})).encode()).hexdigest()

    def _key2path(self, key: str) -> pathlib.Path:
        """Get the filesystem path for the given key.

        Args:
            key: Query key.

        Returns:
            Filesystem path for the cached query.
        """
        return self._path / f'{key}.parquet'

    def exists(self, statement: sql.Selectable) -> bool:
        """Check the result can be provided without execution.

        Args:
            statement: Query statement.

        Returns:
            True if the query result is already known.
        """
        key = self._statement2key(statement)
        return key in self._frames or self._key2path(key).exists()

    def get_or_exec(
        self, statement: sql.Selectable, loader: typing.Callable[[sql.Selectable], pandas.DataFrame]
    ) -> pandas.DataFrame:
        """Get the result from the cache or execute the loader.

        Args:
            statement: Query statement representing the expected result.
            loader: Callback for loading the result data.

        Returns:
            Query result as a Pandas dataframe.
        """
        key = self._statement2key(statement)
        if key not in self._frames:
            path = self._key2path(key)
            if path.exists():
                LOGGER.debug('Disk cache hit for %s', statement)
                frame = pandas.read_parquet(path)
            else:
                LOGGER.debug('Disk cache miss for %s', statement)
                frame = loader(statement)
                frame.to_parquet(path, index=False)
            self._frames[key] = frame
        else:
            LOGGER.debug('Memory cache hit for %s', statement)
        return self._frames[key]


class Feed(io.Feed[sql.Selectable, sql.ColumnElement], alias='alchemy'):
    """Generic SQL feed based on :doc:`SQLAlchemy <sqlalchemy:index>`.

    All the hosted datasets need to be declared using a proper :ref:`content resolver
    <io-resolution>` mapping specified using the ``sources`` option with keys representing the fully
    qualified schema name formatted as ``<full.module.path>:<qualified.Class.Name>`` and the values
    should refer to the physical table names like ``<database>.<table>``.

    Attention:
        All the referenced :ref:`schema catalogs <io-catalog>` must be installed.

    Args:
        sources: The mapping of :ref:`schema catalogs <io-catalog>` to the DB tables.
        readerkw: Optional keywords typically for the :func:`pandas.read_sql
                  <pandas:pandas.read_sql>`.

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [FEED.sql]
        provider = "alchemy"
        connection = "mysql+pymysql://john:smith@localhost/"
        [FEED.sql.sources]
        "openschema.kaggle:Titanic" = "kaggle.titanic"
        "foobar.schemas:Foo.Baz" = "foobar.baz"

    Important:
        Select the ``sql`` :ref:`extras to install <install-extras>` ForML together with the
        SQLAlchemy support.
    """

    _TABLE_NAME = re.compile(r'(?:([\w.]+)\.)?(\w+)')

    class Reader(alchemy.Reader):
        """Using the SQLAlchemy reader as is."""

        RESULTS: Results = Results(setup.USRDIR / '.cache' / 'alchemy')

        @classmethod
        def read(cls, statement: sql.Selectable, **kwargs) -> pandas.DataFrame:
            return cls.RESULTS.get_or_exec(statement, functools.partial(super().read, **kwargs))

    def __init__(
        self,
        sources: typing.Mapping[typing.Union['dsl.Source', str], str],
        **readerkw,
    ):
        def ensure_source(src: typing.Union['dsl.Source', str]) -> 'dsl.Source':
            if isinstance(src, str):
                src = dslmod.Schema.from_path(src)
            return src

        def table(name: str) -> sqlalchemy.TableClause:
            if not (match := self._TABLE_NAME.fullmatch(name)):
                raise forml.InvalidError(f'Invalid table name: {name}')
            schema, name = match.groups()
            return sqlalchemy.table(sql.quoted_name(name, quote=True), schema=schema)

        self._sources: typing.Mapping['dsl.Source', sql.Selectable] = {
            ensure_source(s): table(t) for s, t in sources.items()
        }
        super().__init__(**readerkw)

    @property
    def sources(self) -> typing.Mapping['dsl.Source', sql.Selectable]:
        return types.MappingProxyType(self._sources)
