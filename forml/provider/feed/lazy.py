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
Lazy origin pulling feed implementation.
"""
import abc
import itertools
import logging
import typing

import pandas
import sqlalchemy
from sqlalchemy import sql
from sqlalchemy.engine import interfaces

import forml
from forml.io import dsl, layout
from forml.provider.feed import alchemy

LOGGER = logging.getLogger(__name__)


class _Columns(dsl.Source.Visitor, dsl.Feature.Visitor):
    """Visitor for extracting used columns."""

    def __init__(self):
        self._items: set[dsl.Column] = set()

    @classmethod
    def extract(cls, query: dsl.Query) -> typing.Iterable[tuple[dsl.Table, typing.Collection[dsl.Column]]]:
        """Frontend method for extracting all involved columns from the given query.

        Args:
            query: Query to extract the columns from.

        Return:
            Iterable of tuples of table-grouped columns involved in the query.
        """
        return (
            (t, frozenset(g))
            for t, g in itertools.groupby(sorted(cls()(query), key=lambda c: repr(c.origin)), key=lambda c: c.origin)
        )

    def __call__(self, query: dsl.Query) -> typing.Iterable[dsl.Column]:
        """Apply this visitor to the given query.

        Args:
            query: Query to dissect.

        Returns:
            Set of dsl.Column instances involved in the query.
        """
        self._items = set()
        query.accept(self)
        return frozenset(self._items)

    def visit_element(self, feature: 'dsl.Element') -> None:
        if isinstance(feature, dsl.Column):
            self._items.add(feature)
        else:
            assert isinstance(feature.origin, dsl.Reference)
            if isinstance(feature.origin.instance, dsl.Table):
                self._items.add(dsl.Column(feature.origin.instance, feature.name))
            else:
                feature.origin.instance.accept(self)
        super().visit_element(feature)

    def visit_join(self, source: dsl.Join) -> None:
        if source.condition is not None:
            source.condition.accept(self)
        # self._items.update(dsl.Column.dissect(source.condition))
        super().visit_join(source)

    def visit_query(self, source: dsl.Query) -> None:
        for feature in source.features:
            feature.accept(self)
        # self._items.update(dsl.Column.dissect(*source.features))
        if source.prefilter is not None:
            source.prefilter.accept(self)
            # self._items.update(dsl.Column.dissect(source.prefilter))
        for grouping in source.grouping:
            grouping.accept(self)
        # self._items.update(dsl.Column.dissect(*source.grouping))
        if source.postfilter is not None:
            source.postfilter.accept(self)
            # self._items.update(dsl.Column.dissect(source.postfilter))
        for ordering in source.ordering:
            ordering.feature.accept(self)
            # self._items.update(dsl.Column.dissect(ordering.feature))
        super().visit_query(source)


Partition = typing.TypeVar('Partition')


class Origin(typing.Generic[Partition], metaclass=abc.ABCMeta):
    """Origin base class."""

    DTYPES: typing.Mapping[dsl.Any, type] = {
        dsl.Integer(): int,
        dsl.Float(): float,
        dsl.String(): object,
        dsl.Date(): object,
        dsl.Timestamp(): object,
    }

    def __hash__(self):
        return hash(self.source)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.source == self.source

    def __call__(self, partitions: typing.Iterable[Partition]) -> pandas.DataFrame:
        LOGGER.info('Loading %s', self.key)
        frame = pandas.concat((self.load(p) for p in partitions or [None]), ignore_index=True)
        expected = {f.name: f.kind for f in self.source.features}
        assert (actual := set(frame.columns)).issubset(expected), f'Unexpected column(s): {actual.difference(expected)}'
        return frame.astype({c: self.DTYPES.get(expected[c], expected[c].__type__) for c in frame.columns})

    @property
    def key(self) -> str:
        """Name to be used for internal (unique) referencing.

        Must be a valid identifier -> [_a-zA-Z][_a-zA-Z0-9]*
        """
        return repr(self.source)

    @property
    @abc.abstractmethod
    def source(self) -> dsl.Queryable:
        """The source query this origin provides."""

    @abc.abstractmethod
    def load(self, partition: typing.Optional[Partition]) -> pandas.DataFrame:
        """Content loader.

        Args:
            partition: Partitions to load.

        Returns:
            Data in Pandas DataFrame format.
        """

    def partitions(
        self,
        columns: typing.Collection[dsl.Column],  # pylint: disable=unused-argument
        predicate: typing.Optional[dsl.Predicate],  # pylint: disable=unused-argument
    ) -> typing.Iterable[Partition]:
        """Get the partitions for the data selection.

        Args:
            columns: Iterable of required columns (more can be returned).
            predicate: Optional push-down row filter (mismatching rows can still be returned).

        Returns:
            Iterable of partition identifiers containing the requested data.
        """
        return ()


class Feed(alchemy.Feed):
    """Dummy feed with statically initialized content.

    Static feed is pre-configured with actual data which can only be returned in primitive
    column-wise fashion. No advanced ETL can be applied.
    """

    class Reader(alchemy.Feed.Reader):
        """Extending the SQLAlchemy reader."""

        class Backend(interfaces.Connectable):
            """Serializable in-memory SQLite connection."""

            def __init__(self):
                self._engine: interfaces.Connectable = sqlalchemy.create_engine('sqlite://')

            def __repr__(self):
                return 'LazyReaderBackend'

            def __reduce__(self):
                return self.__class__, ()

            def connect(self, **kwargs):
                return self._engine.connect(**kwargs)

            def execute(self, object_, *multiparams, **params):
                return self._engine.execute(object_, *multiparams, **params)

            def scalar(self, object_, *multiparams, **params):
                return self._engine.scalar(object_, *multiparams, **params)

            # pylint: disable=protected-access
            def _run_visitor(self, visitorcallable, element, **kwargs):
                return self._engine._run_visitor(visitorcallable, element, **kwargs)

            def _execute_clauseelement(self, elem, multiparams=None, params=None):
                return self._engine._execute_clauseelement(elem, multiparams, params)

            def __getattr__(self, item):
                return getattr(self._engine, item)

        def __init__(
            self,
            sources: typing.Mapping[dsl.Source, sql.Selectable],
            features: typing.Mapping[dsl.Feature, sql.ColumnElement],
            origins: typing.Iterable[Origin[Partition]],
        ):
            self._loaded: dict[Origin[Partition], frozenset[Partition]] = {}
            self._origins: dict[dsl.Queryable, Origin[Partition]] = {o.source: o for o in origins}
            self._backend: Feed.Reader.Backend = self.Backend()
            super().__init__(sources, features, self._backend)

        def __call__(self, query: dsl.Query, entry: typing.Optional[layout.Entry] = None) -> layout.Tabular:
            for table, columns in _Columns.extract(query):
                LOGGER.debug('Request for %s using columns: %s', table, columns)
                if table not in self._origins:
                    raise forml.MissingError(f'Unknown origin for table {table}')
                origin = self._origins[table]
                partitions = origin.partitions(columns, None)
                if origin not in self._loaded or not self._loaded[origin].symmetric_difference(partitions):
                    origin(partitions).to_sql(origin.key, self._backend, index=False, if_exists='replace')
                    self._loaded[origin] = frozenset(partitions)
            return super().__call__(query, entry)

    def __init__(self, *origins: Origin[Partition], **readerkw):
        self._sources: typing.Mapping[dsl.Source, sql.Selectable] = {o.source: sqlalchemy.table(o.key) for o in origins}
        super().__init__({o.source: o.key for o in origins}, origins=origins, **readerkw)
