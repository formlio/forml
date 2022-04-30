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
from . import node as nodemod
from . import port


class Visitor(nodemod.Visitor):
    """Path visitor interface."""

    def visit_path(self, path: 'Path') -> None:
        """Path visit.

        Args:
            path: Path visit.
        """


class Traversal(collections.namedtuple('Traversal', 'pivot, members')):
    """Graph traversal helper."""

    pivot: nodemod.Atomic
    """Focal node of this traversal."""
    members: typing.AbstractSet[nodemod.Atomic]
    """All nodes belonging to this traversal (including the 'pivot' node)."""

    class Cyclic(_exception.TopologyError):
        """Cyclic graph error."""

    def __new__(cls, pivot: nodemod.Atomic, members: typing.AbstractSet[nodemod.Atomic] = frozenset()):
        return super().__new__(cls, pivot, frozenset(members | {pivot}))

    def directs(
        self, *extras: nodemod.Atomic, mask: typing.Optional[typing.Callable[[nodemod.Atomic], bool]] = None
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
            (s.node for p in self.pivot.output for s in p), (e for e in extras if e and e.subscribed(self.pivot))
        ):
            if node in seen or mask and not mask(node):
                continue
            if node in self.members:
                raise self.Cyclic(f'Cyclic flow near {node}')
            seen.add(node)
            yield self.__class__(node, self.members)

    def mappers(self, *extras: nodemod.Atomic) -> typing.Iterator['Traversal']:
        """Return subscribers with specific mask to pass only mapper (not trained) nodes.

        Args:
            *extras: Future nodes that might be subscribed to this publisher.

        Returns:
            Subscribers instance.
        """
        return self.directs(*extras, mask=lambda n: not isinstance(n, nodemod.Worker) or not n.trained)

    def tail(self, expected: typing.Optional[nodemod.Atomic] = None) -> 'Traversal':
        """Recursive traversing all mapper subscription paths down to the tail mapper checking there is just one.

        Args:
            expected: Optional indication of the expected tail. If expected is a Future, it's matching Worker is
                      returned instead.

        Returns:
            Tail traversal of the flow.
        """
        if expected and self.pivot == expected:
            return self
        endings = set()
        for node in self.mappers(expected):
            tail = node.tail(expected)
            if expected and tail.pivot == expected:
                return tail
            endings.add(tail)
        if not any(endings):
            return self
        if len(self.members) == 1 and (expected or len(endings) > 1):
            raise _exception.TopologyError('Ambiguous tail')
        return endings.pop()

    def each(self, tail: nodemod.Atomic, acceptor: typing.Callable[[nodemod.Atomic], None]) -> None:
        """Traverse the path downstream calling acceptor for each unique node.

        Potential tail Future node is ignored.

        Args:
            tail: Optional traversal breakpoint.
            acceptor: Acceptor to call for each unique node.
        """

        def unseen(node: nodemod.Atomic) -> bool:
            """Test for node recurrence.

            Args:
                node: Node instance to be checked for recurrence.

            Returns:
                True if not recurrent.
            """
            return node not in seen

        def unseen_trained(node: nodemod.Atomic) -> bool:
            """Mask for trained non-recurrent node.

            Args:
                node: Node instance to be checked.

            Returns:
                True if not recurrent and trained.
            """
            return unseen(node) and isinstance(node, nodemod.Worker) and node.trained

        def traverse(traversal: Traversal) -> None:
            """Recursive path scan.

            Args:
                traversal: Node to be processed.
            """
            mask = unseen_trained if traversal.pivot == tail else unseen
            if isinstance(traversal.pivot, nodemod.Worker) or traversal.pivot != tail:
                acceptor(traversal.pivot)
            seen.add(traversal.pivot)
            for node in traversal.directs(tail, mask=mask):
                traverse(node)

        seen: set[nodemod.Atomic] = set()
        traverse(Traversal(self.pivot))

    def copy(self, tail: nodemod.Atomic) -> typing.Mapping[nodemod.Atomic, nodemod.Atomic]:
        """Make a copy of the apply path topology. Any nodes not on path are ignored.

        Only the main branch is copied ignoring all sink branches.

        Args:
            tail: Last node to copy.

        Returns:
            Copy of the apply path.
        """

        def paths(traversal: Traversal) -> typing.Iterable[Traversal]:
            """Generator of all paths between the current traversal and the tail."""
            if traversal.pivot == tail:
                yield traversal
            else:
                for node in traversal.mappers(tail):
                    yield from paths(node)

        def get(node: nodemod.Atomic) -> nodemod.Atomic:
            """Get the copy of the given node."""
            return copies.get(node) or copies.setdefault(node, node.fork())

        seen: set[tuple[nodemod.Atomic, int, port.Subscription]] = set()
        copies: dict[nodemod.Atomic, nodemod.Atomic] = {}
        get(self.pivot)  # bootstrap for single-node paths that wouldn't iterate through the following loop
        for pub, sub in (
            (get(o)[i], get(s.node)[s.port])
            for t in paths(self)
            for o in t.members
            for i, p in enumerate(o.output)
            for s in p
            if s.node in t.members and (o, i, s) not in seen and not seen.add((o, i, s))
        ):
            sub.subscribe(pub)
        return copies


class Path(tuple):
    """Representing acyclic apply path(s) between two nodes - a sub-graph with single head and tail node each with
    at most one apply input/output port.
    """

    _head: nodemod.Atomic = property(operator.itemgetter(0))
    _tail: nodemod.Atomic = property(operator.itemgetter(1))

    def __new__(cls, head: nodemod.Atomic, tail: typing.Optional[nodemod.Atomic] = None):
        if head.szin > 1:
            raise _exception.TopologyError('Simple head required')
        tail = Traversal(head).tail(tail).pivot
        if tail.szout > 1:
            raise _exception.TopologyError('Simple tail required')
        return super().__new__(cls, (head, tail))

    def is_subpath(self, other: 'Path') -> bool:
        """Check this is a sub-path of the other.

        It is a sub-path if our head is found anywhere on the other path.

        Args:
            other: Path to check against.

        Returns:
            True if this is a sub-path of the other.
        """
        # pylint: disable=protected-access

        def check(node: nodemod.Atomic) -> None:
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
    def root(first: 'Path', *others: 'Path') -> 'Path':
        """Get the root paths amongst the parameters - path that all the others are sub-paths of.

        All paths must be related.

        Args:
            first: Path to start with (syntax to enforce passing at least one path as an argument).
            others: Remaining args of paths from which the root should be selected.
        Returns:
            Root path that all the others are sub-path of.
        """

        def choose(left: Path, right: Path) -> Path:
            """Choose the super-path out of the two.

            Args:
                left: One path to chose from.
                right: The other path to choose from.
            Returns:
                Root path of the two.
            """
            if left.is_subpath(right):
                return right
            if right.is_subpath(left):
                return left
            raise _exception.TopologyError('Unrelated paths.')

        return functools.reduce(choose, others, first)

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        Traversal(self._head).each(self._tail, visitor.visit_node)
        visitor.visit_path(self)

    def extend(
        self,
        right: typing.Optional[typing.Union['Path', nodemod.Atomic]] = None,
        tail: typing.Optional[nodemod.Atomic] = None,
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
            if isinstance(right, nodemod.Atomic):
                right = Path(right)
            right.subscribe(self.publisher)
            if not tail:
                tail = right._tail
        elif not tail:
            tail = Traversal(self._tail).tail().pivot
        return Path(self._head, tail)

    def subscribe(self, publisher: typing.Union[port.Publishable, 'Path']) -> None:
        """Subscribe head node to given publisher."""
        if isinstance(publisher, Path):
            publisher = publisher.publisher
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
