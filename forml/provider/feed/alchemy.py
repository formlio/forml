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
import re
import types
import typing

import sqlalchemy
from sqlalchemy import sql

import forml
from forml import io
from forml.io import dsl as dslmod
from forml.provider.feed.reader.sql import alchemy

if typing.TYPE_CHECKING:
    from forml.io import dsl  # pylint: disable=reimported


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

    def __init__(
        self,
        sources: typing.Mapping[typing.Union['dsl.Source', str], str],
        **readerkw,
    ):
        def ensure_source(src: typing.Union['dsl.Source', str]) -> 'dsl.Source':
            if isinstance(src, str):
                src = dslmod.Schema.from_path(src)
            return src

        def table(name: str) -> sqlalchemy.table:
            if not (match := self._TABLE_NAME.fullmatch(name)):
                raise forml.InvalidError(f'Invalid table name: {name}')
            schema, name = match.groups()
            return sqlalchemy.table(name, schema=schema)

        self._sources: typing.Mapping['dsl.Source', sql.Selectable] = {
            ensure_source(s): table(t) for s, t in sources.items()
        }
        super().__init__(**readerkw)

    @property
    def sources(self) -> typing.Mapping['dsl.Source', sql.Selectable]:
        return types.MappingProxyType(self._sources)
