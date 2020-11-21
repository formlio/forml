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
import itertools
import operator
import typing

from forml.flow import error
from forml.flow.graph import node as grnode, port


class Visitor(grnode.Visitor):
    """Path visitor interface."""

    def visit_path(self, path: 'Path') -> None:
        """Path visit.

        Args:
            path: Path visit.
        """


class Traversal(collections.namedtuple('Traversal', 'current, predecessors')):
    """Graph traversal helper."""

    class Cyclic(error.Topology):
        """Cyclic graph error."""

    def __new__(cls, current: grnode.Atomic, predecessors: typing.AbstractSet[grnode.Atomic] = frozenset()):
        return super().__new__(cls, current, frozenset(predecessors | {current}))

    def directs(
        self, *extras: grnode.Atomic, mask: typing.Optional[typing.Callable[[grnode.Atomic], bool]] = None
    ) -> typing.Iterator['Traversal']:
        """Utility for retrieving set of node subscribers with optional mask and list of potential Futures (that are not
        subscribed directly).

        Args:
            *extras: Future nodes that might be subscribed to this publisher.
            mask: Optional condition for filtering the subscriptions.

        Returns:
            Iterable of new Traversals.
        """
        seen = set()
        for node in itertools.chain(
            (s.node for p in self.current.output for s in p), (e for e in extras if e and e.subscribed(self.current))
        ):
            if node in seen or mask and not mask(node):
                continue
            if node in self.predecessors:
                raise self.Cyclic(f'Cyclic flow near {node}')
            seen.add(node)
            yield self.__class__(node, self.predecessors)

    def mappers(self, *extras: grnode.Atomic) -> typing.Iterator['Traversal']:
        """Return subscribers with specific mask to pass only mapper (not trained) nodes.

        Args:
            *extras: Future nodes that might be subscribed to this publisher.

        Returns:
            Subscribers instance.
        """
        return self.directs(*extras, mask=lambda n: not isinstance(n, grnode.Worker) or not n.trained)

    def tail(self, expected: typing.Optional[grnode.Atomic] = None) -> 'Traversal':
        """Recursive traversing all mapper subscription paths down to the tail mapper checking there is just one.

        Args:
            expected: Optional indication of the expected tail. If expected is a Future, it's matching Worker is
                      returned instead.

        Returns:
            Tail traversal of the flow.
        """
        if expected and self.current == expected:
            return self
        endings = set()
        for node in self.mappers(expected):
            tail = node.tail(expected)
            if expected and tail.current == expected:
                return tail
            endings.add(tail)
        if not any(endings):
            return self
        if len(self.predecessors) == 1 and (expected or len(endings) > 1):
            raise error.Topology('Ambiguous tail')
        return endings.pop()

    def each(self, tail: grnode.Atomic, acceptor: typing.Callable[[grnode.Atomic], None]) -> None:
        """Traverse the path downstream calling acceptor for each unique node.

        Potential tail Future node is ignored.

        Args:
            tail: Optional traversion breakpoint.
            acceptor: Acceptor to call for each unique node.
        """

        def unseen(node: grnode.Atomic) -> bool:
            """Test for node recurrence.

            Args:
                node: Node instance to be checked for recurrence.

            Returns:
                True if not recurrent.
            """
            return node not in seen

        def traverse(traversal: Traversal) -> None:
            """Recursive path scan.

            Args:
                traversal: Node to be processed.
            """
            mask = unseen
            if traversal.current == tail:
                mask = lambda n: unseen(n) and n.trained
            if isinstance(traversal.current, grnode.Worker) or traversal.current != tail:
                acceptor(traversal.current)
            seen.add(traversal.current)
            for node in traversal.directs(tail, mask=mask):
                traverse(node)

        seen = set()
        traverse(Traversal(self.current))

    def copy(self, tail: grnode.Atomic) -> typing.Mapping[grnode.Atomic, grnode.Atomic]:
        """Make a copy of the apply path topology. Any nodes not on path are ignored.

        Only the main branch is copied ignoring all sink branches.

        Args:
            tail: Last node to copy.

        Returns:
            Copy of the apply path.
        """

        def traverse(traversal: Traversal) -> None:
            """Recursive path copy.

            Args:
                traversal: Node to be copied.

            Returns:
                Copy of the publisher node with all of it's subscriptions resolved.
            """
            if traversal.current == tail:
                for orig in traversal.predecessors:
                    pub = copies.get(orig) or copies.setdefault(orig, orig.fork())
                    for index, subscription in (
                        (i, s) for i, p in enumerate(orig.output) for s in p if s.node in traversal.predecessors
                    ):
                        sub = copies.get(subscription.node) or copies.setdefault(
                            subscription.node, subscription.node.fork()
                        )
                        sub[subscription.port].subscribe(pub[index])
            else:
                for node in traversal.mappers(tail):
                    traverse(node)

        copies = dict()
        traverse(self)
        return copies


class Path(tuple):
    """Representing acyclic apply path(s) between two nodes - a sub-graph with single head and tail node each with
    at most one apply input/output port.

    This is a base and factory class for creating specific path instances.
    """

    _head: grnode.Atomic = property(operator.itemgetter(0))
    _tail: grnode.Atomic = property(operator.itemgetter(1))

    def __new__(cls, head: grnode.Atomic, tail: typing.Optional[grnode.Atomic] = None):
        if head.szin > 1:
            raise error.Topology('Simple head required')
        tail = Traversal(head).tail(tail).current
        if tail.szout > 1:
            raise error.Topology('Simple tail required')
        return super().__new__(cls, (head, tail))

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        Traversal(self._head).each(self._tail, visitor.visit_node)
        visitor.visit_path(self)

    def extend(
        self,
        right: typing.Optional[typing.Union['Path', grnode.Atomic]] = None,
        tail: typing.Optional[grnode.Atomic] = None,
    ) -> 'Path':
        """Create new path by appending right head to our tail or retracing this path up to its physical or specified
        tail.

        Args:
            right: Optional path to extend with (retracing to physical or specified tail if not provided).
            tail: Optional tail as a path output vertex.

        Returns:
            New extended path.
        """
        # pylint: disable=protected-access
        if right:
            if isinstance(right, grnode.Atomic):
                right = Path(right)
            right.subscribe(self.publisher)
            if not tail:
                tail = right._tail
        elif not tail:
            tail = Traversal(self._tail).tail().current
        return Path(self._head, tail)

    def subscribe(self, publisher: port.Publishable) -> None:
        """Subscribe head node to given publisher."""
        self._head[0].subscribe(publisher)

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns:
            Publishable tail apply port reference.
        """
        return self._tail[0].publisher

    def copy(self) -> 'Path':
        """Make a copy of the apply path topology. Any nodes not on path are ignored.

        Returns:
            Copy of the apply path.
        """

        copies = Traversal(self._head).copy(self._tail)
        return Path(copies[self._head], copies[self._tail])
