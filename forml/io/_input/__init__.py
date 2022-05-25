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
from forml import flow, provider
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.io.dsl import parser

from . import _producer, extract

if typing.TYPE_CHECKING:
    from forml import project

LOGGER = logging.getLogger(__name__)


class Feed(
    provider.Service,
    typing.Generic[parser.Source, parser.Feature],
    path=provcfg.Feed.path,  # pylint: disable=no-member
):
    """Feed is the implementation of a specific datasource provider.

    Note Feeds need to be serializable!
    """

    Reader = _producer.Reader

    def __init__(self, **readerkw):
        self._readerkw: dict[str, typing.Any] = readerkw

    def load(
        self,
        source: 'project.Source',
        lower: typing.Optional['dsl.Native'] = None,
        upper: typing.Optional['dsl.Native'] = None,
    ) -> flow.Trunk:
        """Provide a pipeline composable segment implementing the etl actions.

        Args:
            source: Independent datasource component.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns:
            Pipeline segment.
        """

        def actor(driver: type[extract.Driver], query: dsl.Query) -> 'flow.Spec[extract.Driver]':
            """Helper for creating the reader actor spec for the given query.

            Args:
                driver: Driver class.
                query: Data loading query.

            Returns:
                Reader actor spec.
            """
            return driver.spec(producer, extract.Statement.prepare(query, source.extract.ordinal, lower, upper))

        producer = self.producer(self.sources, self.features, **self._readerkw)
        apply_actor: flow.Spec[extract.Driver] = actor(extract.RowDriver, source.extract.apply)
        label_actor: typing.Optional[flow.Spec] = None
        train_query: dsl.Query = source.extract.train
        train_driver = extract.RowDriver
        if source.extract.labels:
            train_driver = extract.TableDriver
            if isinstance(source.extract.labels, flow.Spec):
                label_actor = source.extract.labels
            else:
                columns, label_actor = extract.Slicer.from_columns(train_query.features, source.extract.labels)
                train_query = train_query.select(*columns)
        train_actor: flow.Spec[extract.Driver] = actor(train_driver, train_query)
        loader: flow.Composable = extract.Operator(apply_actor, train_actor, label_actor)
        if source.transform:
            loader >>= source.transform
        return loader.expand()

    @classmethod
    def producer(
        cls,
        sources: typing.Mapping['dsl.Source', parser.Source],
        features: typing.Mapping['dsl.Feature', parser.Feature],
        **kwargs: typing.Any,
    ) -> extract.Producer:
        """Return the producer instance of this feed (any callable, presumably _producer.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            features: Column mappings to be used by the reader.
            kwargs: Optional reader keyword arguments.

        Returns:
            Producer instance.
        """
        return cls.Reader(sources, features, **kwargs)  # pylint: disable=abstract-class-instantiated

    @property
    @abc.abstractmethod
    def sources(self) -> typing.Mapping['dsl.Source', parser.Source]:
        """The explicit sources mapping implemented by this feed to be used by the query parser.

        Returns:
            Sources mapping.
        """

    @property
    def features(self) -> typing.Mapping['dsl.Feature', parser.Feature]:
        """The explicit features mapping implemented by this feed to be used by the query parser.

        Returns:
            Features mapping.
        """
        return {}


class Importer:
    """Pool of (possibly) lazily instantiated feeds. If configured without any explicit feeds, all the feeds
    registered in the provider cache are added.

    The pool is used to provide a feed instance that can satisfy particular DSL query (can provide datasource for all
    the schemas used in that query). Feed instances can have priority in which case the first feed with the highest
    priority that's capable of supplying the data is returned.

    TO-DO: This logic should be extended to also probe the available data range so that feed without the expected data
    range is not prioritized over another feed that has the range but has a lower priority.
    """

    class Slot:
        """Representation of a single feed provided either explicitly s an instance or lazily as a descriptor."""

        def __init__(self, feed: typing.Union[provcfg.Feed, str, Feed]):
            if isinstance(feed, str):
                feed = provcfg.Feed.resolve(feed)
            descriptor, instance = (feed, None) if isinstance(feed, provcfg.Feed) else (None, feed)
            self._descriptor: typing.Optional[provcfg.Feed] = descriptor
            self._instance: typing.Optional[Feed] = instance

        def __lt__(self, other: 'Importer.Slot'):
            return self.priority < other.priority

        @property
        def priority(self) -> float:
            """Slots defined explicitly have infinite priority, lazy ones have priority according ot their config.

            Returns:
                Priority value.
            """
            return self._descriptor.priority if self._descriptor else float('inf')

        @property
        def instance(self) -> Feed:
            """Return the feed instance possibly creating it on the fly if lazy.

            Returns:
                Feed instance.
            """
            if self._instance is None:
                LOGGER.debug('Instantiating feed %s', self._descriptor.reference)
                self._instance = Feed[self._descriptor.reference](**self._descriptor.params)
            return self._instance

    class Matcher(dsl.Source.Visitor):
        """Visitor that can be used to determine whether the accepting Frame can be constructed using the provided
        sources. The logic is based on traversing the Frame tree and if hitting a Table (tree leaf) that's not among the
        defined sources it resolves as not matching.
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

    def __init__(self, *feeds: typing.Union[provcfg.Feed, str, Feed]):
        self._feeds: tuple[Importer.Slot] = tuple(sorted((self.Slot(f) for f in feeds or Feed), reverse=True))

    def __iter__(self) -> typing.Iterable[Feed]:
        for feed in self._feeds:
            yield feed.instance

    @functools.cache
    def match(self, source: 'dsl.Source') -> Feed:
        """Select a feed that can provide for (be used to construct) the given source.

        Args:
            source: ETL frame source to be run against the required feed.

        Returns:
            Feed that's able to provide data for the given sources.
        """
        for feed in self:
            matcher = self.Matcher(feed.sources)
            source.accept(matcher)
            if matcher:
                return feed
        raise forml.MissingError(f'None of the {len(self._feeds)} available feeds provide all of the required sources')
