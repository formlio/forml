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
import logging
import typing

from forml import io, error
from forml.conf import provider as conf
from forml.io.dsl.schema import frame, visit

LOGGER = logging.getLogger(__name__)


class Pool:
    """Pool of (possibly) lazily instantiated feeds.
    """
    class Slot:
        """Representation of a single feed provided either explicitly s an instance or lazily as a descriptor.
        """
        def __init__(self, feed: typing.Union[conf.Feed, str, 'io.Feed']):
            if isinstance(feed, str):
                feed = conf.Feed.resolve(feed)
            descriptor, instance = (feed, None) if isinstance(feed, conf.Feed) else (None, feed)
            self._descriptor: typing.Optional[conf.Feed] = descriptor
            self._instance: typing.Optional[io.Feed] = instance

        def __lt__(self, other: 'Pool.Slot'):
            return self.priority < other.priority

        @property
        def priority(self) -> float:
            """Slots defined explicitly have infinite priority, lazy ones have priority according ot their config.

            Returns: Priority value.
            """
            return self._descriptor.priority if self._descriptor else float('inf')

        @property
        def instance(self) -> 'io.Feed':
            """Return the feed instance possibly creating it on the fly if lazy.

            Returns: Feed instance.
            """
            if self._instance is None:
                LOGGER.debug('Instantiating feed %s', self._descriptor.reference)
                self._instance = io.Feed[self._descriptor.reference](**self._descriptor.params)
            return self._instance

    class Matcher(visit.Frame):
        """Visitor that can be used to determine whether the accepting Frame can be constructed using the provided
        sources. The logic is based on traversing the Frame tree and if hitting a Table (tree leaf) that's not among the
        defined sources it resolves as not matching.
        """
        def __init__(self, sources: typing.Iterable[frame.Source]):
            self._sources: typing.FrozenSet[frame.Source] = frozenset(sources)
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

        def visit_table(self, source: frame.Table) -> None:
            if source not in self._sources:
                self._matches = False

    def __init__(self, *feeds: typing.Union[conf.Feed, str, 'io.Feed']):
        self._feeds: typing.Tuple[Pool.Slot] = tuple(sorted((self.Slot(f) for f in feeds), reverse=True))

    def __iter__(self) -> typing.Iterable['io.Feed']:
        for feed in self._feeds:
            yield feed.instance

    def match(self, source: frame.Source) -> 'io.Feed':
        """Select a feed that can provide for (be used to construct) the given source.

        Args:
            source: ETL frame source to be run against the required feed.

        Returns: Feed that's able to provide data for the given sources.
        """
        for feed in self:
            matcher = self.Matcher(feed.sources)
            source.accept(matcher)
            if matcher:
                break
        else:
            raise error.Missing(f'None of the {len(self._feeds)} available feeds provide all of the required sources')
        return feed  # pylint: disable=undefined-loop-variable
