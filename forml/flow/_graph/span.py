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
Graph view - useful lenses and manipulation of graph topology parts.
"""

import collections
import functools
import itertools
import operator
import typing

from .. import _exception
from . import atomic, port

if typing.TYPE_CHECKING:
    from forml import flow


class Visitor(atomic.Visitor):
    """Segment visitor interface."""

    def visit_segment(self, segment: 'flow.Segment') -> None:
        """Segment visit.

        Args:
            segment: Segment visit.
        """


class Traversal(collections.namedtuple('Traversal', 'pivot, members')):
    """Graph traversal helper."""

    pivot: 'flow.Node'
    """Focal node of this traversal."""
    members: typing.AbstractSet['flow.Node']
    """All nodes belonging to this traversal (including the 'pivot' node)."""

    class Cyclic(_exception.TopologyError):
        """Cyclic graph error."""

    def __new__(cls, pivot: 'flow.Node', members: typing.AbstractSet['flow.Node'] = frozenset()):
        return super().__new__(cls, pivot, frozenset(members | {pivot}))

    def subscribers(
        self, *extras: 'flow.Node', mask: typing.Optional[typing.Callable[['flow.Node'], bool]] = None
    ) -> typing.Iterator['Traversal']:
        """Utility for retrieving set of node subscribers with optional mask and list of potential
        Futures (that are not subscribed directly).

        Args:
            *extras: Future nodes that might be subscribed to this publisher.
            mask: Optional condition for filtering the subscriptions.

        Returns:
            Iterable of new Traversals.
        """
        seen = set()
        for node in itertools.chain(
            (s.node for p in self.pivot.output for s in p), (e for e in extras if e.subscribed(self.pivot))
        ):
            if node in seen or mask and not mask(node):
                continue
            if node in self.members:
                raise self.Cyclic(f'Cyclic flow near {node}')
            seen.add(node)
            yield self.__class__(node, self.members)

    def mappers(self, *extras: 'flow.Node') -> typing.Iterator['Traversal']:
        """Return subscribers with specific mask to pass only mapper (not trained) nodes.

        Args:
            *extras: Future nodes that might be subscribed to this publisher.

        Returns:
            Subscribers instance.
        """
        return self.subscribers(*extras, mask=lambda n: not isinstance(n, atomic.Worker) or not n.trained)

    def tail(self, expected: typing.Optional['flow.Node'] = None) -> 'Traversal':
        """Recursive traversing all mapper subscription segments down to the tail mapper checking
        there is just one.

        Args:
            expected: Optional indication of the expected tail - it's an error if not found.

        Returns:
            Tail traversal of the flow.
        """

        def exists(traversal: Traversal) -> bool:
            """Traverse the graph and return True if the *expected* node is found."""
            if traversal.pivot == expected:
                return True
            return any(exists(m) for m in traversal.mappers(expected))

        def scan(traversal: Traversal) -> set[Traversal]:
            """Traverse the graph and return set of all leaf nodes."""
            return {e for m in traversal.mappers() for e in scan(m)} or {traversal}

        if expected:
            if exists(self):
                return Traversal(expected)
            raise _exception.TopologyError(f'Disconnected tail {expected}')

        leaves = scan(self)
        if len(leaves) > 1:
            raise _exception.TopologyError('Ambiguous tail')
        return leaves.pop()

    def each(self, tail: 'flow.Node', acceptor: typing.Callable[['flow.Node'], None]) -> None:
        """Traverse the segment downstream calling acceptor for each unique node.

        Potential tail Future node is ignored.

        Args:
            tail: Optional traversal breakpoint.
            acceptor: Acceptor to call for each unique node.
        """

        def unseen(node: 'flow.Node') -> bool:
            """Test for node recurrence.

            Args:
                node: Node instance to be checked for recurrence.

            Returns:
                True if not recurrent.
            """
            return node not in seen

        def unseen_trained(node: 'flow.Node') -> bool:
            """Mask for trained non-recurrent node.

            Args:
                node: Node instance to be checked.

            Returns:
                True if not recurrent and trained.
            """
            return unseen(node) and isinstance(node, atomic.Worker) and node.trained

        def traverse(traversal: Traversal) -> None:
            """Recursive segment scan.

            Args:
                traversal: Node to be processed.
            """
            mask = unseen_trained if traversal.pivot == tail else unseen
            if isinstance(traversal.pivot, atomic.Worker) or traversal.pivot != tail:
                acceptor(traversal.pivot)
            seen.add(traversal.pivot)
            for node in traversal.subscribers(tail, mask=mask):
                traverse(node)

        seen: set['flow.Node'] = set()
        traverse(Traversal(self.pivot))

    def copy(self, tail: 'flow.Node') -> typing.Mapping['flow.Node', 'flow.Node']:
        """Make a copy of the *apply-mode* topology.

        Any trained nodes as well as nodes outside the segment are ignored (only the direct branch
        is copied ignoring all sink branches). Copied nodes remain members of the same worker
        groups.

        Args:
            tail: Last node to copy.

        Returns:
            Copy of the *apply-mode* segment topology.
        """

        def segments(traversal: Traversal) -> typing.Iterable[Traversal]:
            """Generator of all segments between the current traversal and the tail."""
            if traversal.pivot == tail:
                yield traversal
            else:
                for node in traversal.mappers(tail):
                    yield from segments(node)

        def get(node: 'flow.Node') -> 'flow.Node':
            """Get the copy of the given node."""
            return copies.get(node) or copies.setdefault(node, node.fork())

        seen: set[tuple['flow.Node', int, port.Subscription]] = set()
        copies: dict['flow.Node', 'flow.Node'] = {}
        get(self.pivot)  # bootstrap for single-node segments that wouldn't iterate through the following loop
        for pub, sub in (
            (get(o)[i], get(s.node)[s.port])
            for t in segments(self)
            for o in t.members
            for i, p in enumerate(o.output)
            for s in p
            if s.node in t.members and (o, i, s) not in seen and not seen.add((o, i, s))
        ):
            sub.subscribe(pub)
        return copies


class Segment(tuple):
    """Representing an acyclic (sub)graph between two apply-mode nodes.

    Each of the two boundary nodes must be externally facing with just a *single port* (``.head``
    node having a single input port and ``.tail`` node having a single output port).

    The ``tail`` node (if provided) must be reachable from the ``head`` node via the existing
    connections.

    Args:
        head: The first (input) node in this segment.
        tail: The last (output) node in this segment (auto-traced if not provided).
    """

    _head: 'flow.Node' = property(operator.itemgetter(0))
    _tail: 'flow.Node' = property(operator.itemgetter(1))

    def __new__(cls, head: 'flow.Node', tail: typing.Optional['flow.Node'] = None):
        if head.szin > 1:
            raise _exception.TopologyError(f'Simple head required - got {head} with {head.szin} ports')
        tail = Traversal(head).tail(tail).pivot
        if tail.szout > 1:
            raise _exception.TopologyError(f'Simple tail required - got {tail} with {tail.szout} ports')
        return super().__new__(cls, (head, tail))

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        Traversal(self._head).each(self._tail, visitor.visit_node)
        visitor.visit_segment(self)

    @property
    def publisher(self) -> 'flow.Publishable':
        """Publishable tail node representation.

        Returns:
            Publishable tail *Apply* port reference.
        """
        return self._tail[0].publisher

    def subscribe(self, publisher: typing.Union['flow.Publishable', 'flow.Segment']) -> None:
        """Subscribe our head node to the given publisher.

        Args:
            publisher: Another segment or a general publisher to subscribe to.
        """
        if isinstance(publisher, Segment):
            publisher = publisher.publisher
        self._head[0].subscribe(publisher)

    def extend(
        self,
        right: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        tail: typing.Optional['flow.Node'] = None,
    ) -> 'flow.Segment':
        """Create a new segment by appending the right head to our tail or retracing this segment up
        to its physical or explicit tail.

        Args:
            right: An optional segment to extend with (retracing to the physical or explicit tail if
                   not provided).
            tail: An optional tail as a segment exit node.

        Returns:
            New extended segment.
        """
        # pylint: disable=protected-access
        if right:
            if isinstance(right, atomic.Node):
                right = Segment(right)
            right.subscribe(self.publisher)
            if not tail:
                tail = right._tail
        elif not tail:
            tail = Traversal(self._tail).tail().pivot
        return Segment(self._head, tail)

    def copy(self) -> 'flow.Segment':
        """Make a copy of the *apply-mode* topology within this segment (all trained nodes are
        ignored).

        Copied nodes remain members of the same worker groups.

        Returns:
            Copy of the *Apply* segment topology.
        """

        copies = Traversal(self._head).copy(self._tail)
        return Segment(copies[self._head], copies[self._tail])

    def follows(self, other: 'flow.Segment') -> bool:
        """Check this segment follows from the other.

        It follows, if our head is found anywhere within the other segment.

        Args:
            other: Segment to check against.

        Returns:
            True if this follows from the other.
        """
        # pylint: disable=protected-access

        def check(node: 'flow.Node') -> None:
            """Check the node is our head node.

            Args:
                node: Graph node to check for being this head.
            """
            nonlocal result
            if not result and node is self._head:
                result = True

        result = False
        Traversal(other._head).each(self._head, check)
        return result

    @staticmethod
    def root(first: 'flow.Segment', *others: 'flow.Segment') -> 'flow.Segment':
        """Get the root segments amongst the parameters - segment that all the others follow from.

        All segments must be related.

        Args:
            first: Segment to start with (syntax to enforce passing at least one segment as an
                   argument).
            others: Remaining args of segments from which the root should be selected.
        Returns:
            Root segment that all the others follow from.
        """

        def choose(left: Segment, right: Segment) -> Segment:
            """Choose the super-segment out of the two.

            Args:
                left: One segment to chose from.
                right: The other segment to choose from.
            Returns:
                Root segment of the two.
            """
            if left.follows(right):
                return right
            if right.follows(left):
                return left
            raise _exception.TopologyError('Unrelated segments.')

        return functools.reduce(choose, others, first)
