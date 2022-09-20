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
IO feed utils.
"""
import abc
import functools
import logging
import typing

import forml
from forml import flow, provider, setup
from forml.io import dsl as dslmod
from forml.io.dsl import parser as parsmod

from . import _producer
from . import extract as extmod

if typing.TYPE_CHECKING:
    from forml import io, project
    from forml.io import dsl  # pylint: disable=reimported
    from forml.io.dsl import parser  # pylint: disable=reimported

LOGGER = logging.getLogger(__name__)


class Feed(
    provider.Service,
    typing.Generic[parsmod.Source, parsmod.Feature],
    path=setup.Feed.path,  # pylint: disable=no-member
):
    """Abstract base class for data source feed providers.

    It integrates the concept of a DSL-based ``Reader`` provided using the :meth:`producer` method
    or by overriding the inner ``.Reader`` class and a *content resolver* with its abstract parts
    represented by the :attr:`sources` and :attr:`features` properties.

    The need for implementing the content resolver mapping specifically for each particular platform
    makes it more difficult to setup Feed providers using just the parametric configuration and
    might end up requiring to actually implement the Feed (or at least the final resolver part)
    explicitly as a code.

    Important:
        Feeds need to be serializable!
    """

    Reader = _producer.Reader

    def __init__(self, **readerkw):
        self._readerkw: dict[str, typing.Any] = readerkw

    def load(
        self,
        extract: 'project.Source.Extract',
        lower: typing.Optional['dsl.Native'] = None,
        upper: typing.Optional['dsl.Native'] = None,
    ) -> flow.Composable:
        """Provide a pipeline composable implementing the extract action.

        Args:
            extract: Datasource extract component.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns:
            Pipeline extract composable.
        """

        def actor(driver: type[extmod.Driver], statement: 'dsl.Statement') -> 'flow.Builder[extmod.Driver]':
            """Helper for creating the reader actor builder for the given query.

            Args:
                driver: Driver class.
                statement: Data loading query.

            Returns:
                Reader actor builder.
            """
            return driver.builder(producer, extmod.Statement.prepare(statement, extract.ordinal, lower, upper))

        producer = self.producer(self.sources, self.features, **self._readerkw)
        apply_actor: flow.Builder[extmod.Driver] = actor(extmod.RowDriver, extract.apply)
        label_actor: typing.Optional[flow.Builder] = None
        train_statement: 'dsl.Statement' = extract.train
        train_driver = extmod.RowDriver
        if extract.labels:
            train_driver = extmod.TableDriver
            if isinstance(extract.labels, flow.Builder):
                label_actor = extract.labels
            else:
                columns, label_actor = extmod.Slicer.from_columns(train_statement.features, extract.labels)
                train_statement = train_statement.select(*columns)
        train_actor: flow.Builder[extmod.Driver] = actor(train_driver, train_statement)
        return extmod.Operator(apply_actor, train_actor, label_actor)

    @classmethod
    def producer(
        cls,
        sources: typing.Mapping['dsl.Source', 'parser.Source'],
        features: typing.Mapping['dsl.Feature', 'parser.Feature'],
        **kwargs: typing.Any,
    ) -> 'io.Producer':
        """Producer factory method.

        A ``Producer`` is a generic callable interface most typically represented using the
        :class:`forml.io.Feed.Reader` implementation whose task is to parse the provided DSL query
        and resolve it using its linked storage.

        Unless overloaded, the method returns an instance of ``cls.Reader`` (which might be easier
        to extend without needing to overload this method).

        Args:
            sources: Source mappings to be used by the reader (see :attr:`sources`).
            features: Column mappings to be used by the reader (see :attr:`features`).
            kwargs: Optional reader keyword arguments.

        Returns:
            Producer instance.
        """
        return cls.Reader(sources, features, **kwargs)  # pylint: disable=abstract-class-instantiated

    @property
    @abc.abstractmethod
    def sources(self) -> typing.Mapping['dsl.Source', 'parser.Source']:
        """The main part of the *content resolver* providing the *Source* mappings.

        This way the Feed is advertising the available datasets represented using their
        :ref:`published schemas <io-catalog>` logically mapped to the hosted data sources specified
        using the *parser-specific* semantics.

        A :class:`Source <forml.io.dsl.Source>` is a DSL concept representing anything that can be
        queried as a data source.

        Returns:
            Sources mapping.

        Examples:
            Using a parser with ``SQLAlchemy`` semantics, an example of the mapping might look like
            this::

                return {
                    schema.Titanic: sqlalchemy.table('titanic'),
                    foo.Bar.join(foo.Baz, foo.Bar.id == foo.Baz.id): sqlalchemy.table('foobar'),
                }

            Note the capability of mapping a complex query to a denormalized dataset.
        """

    @property
    def features(self) -> typing.Mapping['dsl.Feature', 'parser.Feature']:
        """The minor part of the *content resolver* providing the optional *Feature* mappings.

        Optional mapping of individual Features to their hosted representation using the
        *parser-specific* semantic.

        A :class:`Feature <forml.io.dsl.Feature>` is a DSL concept representing anything that
        can be projected to a data column.

        Returns:
            Features mapping.
        """
        return {}


class Importer:
    """Pool of (possibly) lazily instantiated feeds.

    The pool is used to select the most suitable feed instance capable of resolving the particular
    DSL query (in terms of providing data sources for all the involved schemas). Feed instances can
    have a static priority assigned in which case the first feed with the highest priority capable
    of providing the data is returned.

    If configured without any explicit instances, all the feeds registered in the provider cache are
    pooled.

    Todo:
        This logic should be extended to also probe the available data range so that a feed without
        the expected data range is not prioritized over another feed that has the range but has a
        lower absolute priority.
    """

    class Slot:
        """Representation of a single feed provided either explicitly s an instance or lazily as
        a descriptor."""

        def __init__(self, feed: typing.Union[setup.Feed, str, 'io.Feed']):
            if isinstance(feed, str):
                feed = setup.Feed.resolve(feed)
            descriptor, instance = (feed, None) if isinstance(feed, setup.Feed) else (None, feed)
            self._descriptor: typing.Optional[setup.Feed] = descriptor
            self._instance: typing.Optional[Feed] = instance

        def __lt__(self, other: 'io.Importer.Slot'):
            return self.priority < other.priority

        @property
        def priority(self) -> float:
            """Slots defined explicitly have infinite priority, lazy ones have priority according
            to their setup.

            Returns:
                Priority value.
            """
            return self._descriptor.priority if self._descriptor else float('inf')

        @property
        def instance(self) -> 'io.Feed':
            """Return the feed instance possibly creating it on the fly if lazy.

            Returns:
                Feed instance.
            """
            if self._instance is None:
                LOGGER.debug('Instantiating feed %s', self._descriptor.reference)
                self._instance = Feed[self._descriptor.reference](**self._descriptor.params)
            return self._instance

    class Matcher(dslmod.Source.Visitor):
        """Visitor that can be used to determine whether the accepting Frame can be constructed
        using the provided sources. The logic is based on traversing the Frame tree and if hitting
        a Table (tree leaf) that's not among the defined sources it resolves as not matching.
        """

        def __init__(self, sources: typing.Iterable['dsl.Source']):
            self._sources: frozenset['dsl.Source'] = frozenset(sources)
            self._matches: bool = True

        def __bool__(self):
            return self._matches

        def visit_reference(self, source: 'dsl.Reference') -> None:
            if self and source not in self._sources:
                super().visit_reference(source)

        def visit_join(self, source: 'dsl.Join') -> None:
            if self and source not in self._sources:
                super().visit_join(source)

        def visit_set(self, source: 'dsl.Set') -> None:
            if self and source not in self._sources:
                super().visit_set(source)

        def visit_query(self, source: 'dsl.Query') -> None:
            if self and source not in self._sources:
                super().visit_query(source)

        def visit_table(self, source: 'dsl.Table') -> None:
            if source not in self._sources:
                self._matches = False

    def __init__(self, *feeds: typing.Union['setup.Feed', str, 'io.Feed']):
        self._feeds: tuple[Importer.Slot] = tuple(sorted((self.Slot(f) for f in feeds or Feed), reverse=True))

    def __iter__(self) -> typing.Iterable['io.Feed']:
        for feed in self._feeds:
            yield feed.instance

    @functools.lru_cache
    def match(self, source: 'dsl.Source') -> 'io.Feed':
        """Select a feed instance that can supply data for the given ``source``.

        Args:
            source: Any *DSL Source* representing the data request.

        Returns:
            Feed that's able to provide data for the given request.

        Raises:
            forml.MissingError: If no feed can provide the requested data.
        """
        for feed in self:
            matcher = self.Matcher(feed.sources)
            source.accept(matcher)
            if matcher:
                return feed
        raise forml.MissingError(f'None of the {len(self._feeds)} available feeds provide all of the required sources')
