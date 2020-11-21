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

from forml import error
from forml import provider as provmod
from forml.conf.parsed import provider as provcfg
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.io import payload
from forml.io.dsl import parser
from forml.io.dsl.struct import visit
from forml.io.feed import extract
from forml.project import component

if typing.TYPE_CHECKING:
    from forml.io.dsl.struct import series, frame, kind as kindmod


LOGGER = logging.getLogger(__name__)


class Provider(provmod.Interface, typing.Generic[parser.Source, parser.Column], path=provcfg.Feed.path):
    """Feed is the implementation of a specific datasource provider."""

    class Reader(extract.Reader[parser.Source, parser.Column, payload.Native], metaclass=abc.ABCMeta):
        """Abstract reader of the feed."""

    def __init__(self, **readerkw):
        self._readerkw: typing.Dict[str, typing.Any] = readerkw

    def load(
        self,
        source: 'component.Source',
        lower: typing.Optional['kindmod.Native'] = None,
        upper: typing.Optional['kindmod.Native'] = None,
    ) -> pipeline.Segment:
        """Provide a pipeline composable segment implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns:
            Pipeline segment.
        """

        def formatter(provider: typing.Callable[..., payload.ColumnMajor]) -> typing.Callable[..., typing.Any]:
            """Creating a closure around the provider with the custom formatting to be applied on the provider output.

            Args:
                provider: Original provider whose output is to be formatted.

            Returns:
                Wrapper that applies formatting upon calling the provider.
            """

            @functools.wraps(provider)
            def wrapper(*args, **kwargs) -> typing.Any:
                """Wrapped provider with custom formatting.

                Args:
                    *args: Original args.
                    **kwargs: Original kwargs.

                Returns:
                    Formatted data.
                """
                return self.format(provider(*args, **kwargs))

            return wrapper

        def actor(handler: typing.Callable[..., typing.Any], spec: 'frame.Query') -> task.Spec:
            """Helper for creating the reader actor spec for given query.

            Args:
                handler: Reading handler.
                spec: Data loading statement.

            Returns:
                Reader actor spec.
            """
            return extract.Reader.Actor.spec(
                handler, extract.Statement.prepare(spec, source.extract.ordinal, lower, upper)
            )

        reader = self.reader(self.sources, self.columns, **self._readerkw)
        query: 'frame.Query' = source.extract.train
        label: typing.Optional[task.Spec] = None
        if source.extract.labels:  # trainset/label formatting is applied only after label extraction
            query = query.select(*(*source.extract.train.columns, *source.extract.labels))
            label = extract.Slicer.Actor.spec(
                formatter(self.slicer(query.columns, self.columns)), source.extract.train.columns, source.extract.labels
            )
        else:  # testset formatting is applied straight away
            reader = formatter(reader)
        train = actor(reader, query)
        apply = actor(formatter(self.reader(self.sources, self.columns, **self._readerkw)), source.extract.apply)
        loader: topology.Composable = extract.Operator(apply, train, label)
        if source.transform:
            loader >>= source.transform
        return loader.expand()

    @classmethod
    def reader(
        cls,
        sources: typing.Mapping['frame.Source', parser.Source],
        columns: typing.Mapping['series.Column', parser.Column],
        **kwargs: typing.Any,
    ) -> typing.Callable[['frame.Query'], payload.ColumnMajor]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.
            kwargs: Optional reader keyword arguments.

        Returns:
            Reader instance.
        """
        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def slicer(
        cls, schema: typing.Sequence['series.Column'], columns: typing.Mapping['series.Column', parser.Column]
    ) -> typing.Callable[[payload.ColumnMajor, typing.Union[slice, int]], payload.ColumnMajor]:
        """Return the slicer instance of this feed, that is able to split the loaded dataset column-wise.

        This default slicer is plain positional sequence slicer.

        Args:
            schema: List of expected columns to be sliced from.
            columns: Column mappings to be used by the selector.

        Returns:
            Slicer instance.
        """
        return extract.Slicer(schema, columns)

    @classmethod
    def format(cls, data: payload.ColumnMajor) -> typing.Any:
        """Optional post-formatting to be applied upon obtaining the columnar data from the raw reader.

        Args:
            data: Input Columnar data to be formatted.

        Returns:
            Formatted data.
        """
        return data

    @property
    @abc.abstractmethod
    def sources(self) -> typing.Mapping['frame.Source', parser.Source]:
        """The explicit sources mapping implemented by this feed to be used by the query parser.

        Returns:
            Sources mapping.
        """

    @property
    def columns(self) -> typing.Mapping['series.Column', parser.Column]:
        """The explicit columns mapping implemented by this feed to be used by the query parser.

        Returns:
            Columns mapping.
        """
        return {}


class Pool:
    """Pool of (possibly) lazily instantiated feeds. If configured without any explicit feeds, all of the feeds
    registered in the provider cache are added.

    The pool is used to provide a feed instance that can satisfy particular DSL query (can provide datasource for all of
    the schemas used in that query). Feed instances can have priority in which case the first feed with highest priority
    that's capable of supplying the data is returned.

    TO-DO: This logic should be extended to also probe the available data range so that feed without the expected data
    range is not prioritized over another feed that has the range but has a lower priority.
    """

    class Slot:
        """Representation of a single feed provided either explicitly s an instance or lazily as a descriptor."""

        def __init__(self, feed: typing.Union[provcfg.Feed, str, Provider]):
            if isinstance(feed, str):
                feed = provcfg.Feed.resolve(feed)
            descriptor, instance = (feed, None) if isinstance(feed, provcfg.Feed) else (None, feed)
            self._descriptor: typing.Optional[provcfg.Feed] = descriptor
            self._instance: typing.Optional[Provider] = instance

        def __lt__(self, other: 'Pool.Slot'):
            return self.priority < other.priority

        @property
        def priority(self) -> float:
            """Slots defined explicitly have infinite priority, lazy ones have priority according ot their config.

            Returns:
                Priority value.
            """
            return self._descriptor.priority if self._descriptor else float('inf')

        @property
        def instance(self) -> Provider:
            """Return the feed instance possibly creating it on the fly if lazy.

            Returns:
                Feed instance.
            """
            if self._instance is None:
                LOGGER.debug('Instantiating feed %s', self._descriptor.reference)
                self._instance = Provider[self._descriptor.reference](**self._descriptor.params)
            return self._instance

    class Matcher(visit.Frame):
        """Visitor that can be used to determine whether the accepting Frame can be constructed using the provided
        sources. The logic is based on traversing the Frame tree and if hitting a Table (tree leaf) that's not among the
        defined sources it resolves as not matching.
        """

        def __init__(self, sources: typing.Iterable['frame.Source']):
            self._sources: typing.FrozenSet['frame.Source'] = frozenset(sources)
            self._matches: bool = True

        def __bool__(self):
            return self._matches

        def visit_reference(self, source: 'frame.Reference') -> None:
            if self and source not in self._sources:
                super().visit_reference(source)

        def visit_join(self, source: 'frame.Join') -> None:
            if self and source not in self._sources:
                super().visit_join(source)

        def visit_set(self, source: 'frame.Set') -> None:
            if self and source not in self._sources:
                super().visit_set(source)

        def visit_query(self, source: 'frame.Query') -> None:
            if self and source not in self._sources:
                super().visit_query(source)

        def visit_table(self, source: 'frame.Table') -> None:
            if source not in self._sources:
                self._matches = False

    def __init__(self, *feeds: typing.Union[provcfg.Feed, str, Provider]):
        self._feeds: typing.Tuple[Pool.Slot] = tuple(sorted((self.Slot(f) for f in feeds or Provider), reverse=True))

    def __iter__(self) -> typing.Iterable[Provider]:
        for feed in self._feeds:
            yield feed.instance

    def match(self, source: 'frame.Source') -> Provider:
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
                break
        else:
            raise error.Missing(f'None of the {len(self._feeds)} available feeds provide all of the required sources')
        return feed  # pylint: disable=undefined-loop-variable
