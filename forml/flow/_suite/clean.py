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
Graph topology validation.

# train and apply graph don't intersect (enforced synchronously)
# acyclic (enforced synchronously)
# nodes ports subscriptions:
# * train/apply subscriptions are exclusive (enforced synchronously)
# * no future nodes
# * at most single trained node per each instance (enforced synchronously)
# * either both train and label or all apply inputs and outputs are active (enforced synchronously)
"""
import typing
import uuid

from .. import _exception
from .._graph import atomic, span
from . import member

if typing.TYPE_CHECKING:
    from forml import flow


class Stateful(span.Visitor, typing.Sequence[uuid.UUID]):
    """Visitor that cumulates gids of stateful nodes."""

    def __init__(self):
        self._gids: list[uuid.UUID] = []

    def __getitem__(self, index: int) -> uuid.UUID:
        return self._gids[index]

    def __len__(self) -> int:
        return len(self._gids)

    def visit_node(self, node: 'flow.Worker') -> None:
        if isinstance(node, atomic.Worker) and node.derived and node.gid not in self._gids:
            self._gids.append(node.gid)


class Stateless(member.Composable):
    """Composable wrapper that ensures there are no stateful actors involved in the wrapped
    composable."""

    def __init__(self, wrapped: 'flow.Composable'):
        self._wrapped: 'flow.Composable' = wrapped

    @classmethod
    def ensure(cls, segment: 'flow.Segment') -> 'flow.Segment':
        """Ensure there are no stateful nodes in the given segment.

        Args:
            segment: Segment to be checked.

        Returns:
            The checked segment.

        Raises:
            flow.TopologyError: If the segment contains any stateful nodes.
        """
        stateful = Stateful()
        segment.accept(stateful)
        if stateful:
            raise _exception.TopologyError('Illegal use of stateful node')
        return segment

    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        return self._wrapped.compose(scope)

    def expand(self) -> 'flow.Trunk':
        trunk = self._wrapped.expand()
        for segment in trunk:
            self.ensure(segment)
        return trunk


class Validator(span.Visitor):
    """Visitor ensuring all nodes are in valid state which means:

    * are Worker instances (not Future)
    """

    def __init__(self):
        self._futures: set['flow.Node'] = set()

    def visit_node(self, node: atomic.Node) -> None:
        """Node visit.

        Args:
            node: Node to be visited.
        """
        if isinstance(node, atomic.Future):
            self._futures.add(node)

    def visit_segment(self, segment: 'flow.Segment') -> None:
        """Final visit.

        Args:
            segment: Segment to be visited.
        """
        if self._futures:
            raise _exception.TopologyError(f'Future nodes in segment: {", ".join(str(f) for f in self._futures)}')
