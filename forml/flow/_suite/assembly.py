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
ForML pipeline composition logic.
"""
import collections
import functools
import typing
import uuid

from .._graph import atomic, span
from . import clean

if typing.TYPE_CHECKING:
    from forml import flow


class Trunk(collections.namedtuple('Trunk', 'apply, train, label')):
    """Structure for integrating the three related segments representing both the runtime modes."""

    apply: 'flow.Segment'
    train: 'flow.Segment'
    label: 'flow.Segment'

    def __new__(
        cls,
        apply: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        train: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        label: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
    ):
        def init(mode: typing.Optional[typing.Union['flow.Segment', 'flow.Node']]) -> span.Segment:
            """Apply default cleaning to the mode segment.

            Args:
                mode: Mode segment provided either as a segment or just a node.

            Returns:
                Cleaned mode segment.
            """
            if not mode:
                mode = atomic.Future()
            if isinstance(mode, atomic.Node):
                mode = span.Segment(mode)
            return mode

        return super().__new__(cls, init(apply), init(train), init(label))

    def extend(
        self,
        apply: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        train: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        label: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
    ) -> 'flow.Trunk':
        """Helper for creating new Trunk with the specified segments *extended* by the provided values.

        Args:
            apply: Optional segment to extend our existing *apply* segment with.
            train: Optional segment to extend our existing *train* segment with.
            label: Optional segment to extend our existing *label* segment with.

        Returns:
            New Trunk instance.
        """
        return self.__class__(
            self.apply.extend(apply) if apply else self.apply,
            self.train.extend(train) if train else self.train,
            self.label.extend(label) if label else self.label,
        )

    def use(
        self,
        apply: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        train: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
        label: typing.Optional[typing.Union['flow.Segment', 'flow.Node']] = None,
    ) -> 'flow.Trunk':
        """Helper for creating new Trunk with the specified segments *replaced* by the provided values.

        Args:
            apply: Optional segment to replace our existing *apply* segment with.
            train: Optional segment to replace our existing *train* segment with.
            label: Optional segment to replace our existing *label* segment with.

        Returns:
            New Trunk instance.
        """
        return self.__class__(apply or self.apply, train or self.train, label or self.label)


class Composition(collections.namedtuple('Composition', 'apply, train')):
    """Structure for holding related flow parts of different modes."""

    apply: 'flow.Segment'
    train: 'flow.Segment'

    class Persistent(span.Visitor, typing.Iterable[uuid.UUID]):
        """Visitor that cumulates gids of persistent nodes."""

        def __init__(self):
            self._gids: list[uuid.UUID] = []

        def __iter__(self) -> typing.Iterator[uuid.UUID]:
            return iter(self._gids)

        def visit_node(self, node: atomic.Worker) -> None:
            if node.derived and node.gid not in self._gids:
                self._gids.append(node.gid)

    def __new__(cls, *segments: Trunk):
        segments = iter(segments)
        composed = next(segments)
        for other in segments:
            composed = composed.extend(*other)

        apply = composed.apply.extend()
        # apply.accept(clean.Validator())
        train = composed.train.extend()
        train.accept(clean.Validator())
        # label = composed.label.extend()
        # label.accept(clean.Validator())
        return super().__new__(cls, apply, train)

    @functools.cached_property
    def persistent(self) -> typing.Sequence[uuid.UUID]:
        """Get the set of nodes with state that needs to be carried over between the apply/train modes.

        The states used within the apply segment are expected to be subset of the states used in the train segment
        (since not all the stateful workers engaged during training are necessarily used during apply and hence don't
        need persisting so we can ignore them).

        Returns:
            Set of nodes sharing state between pipeline modes.
        """
        apply = self.Persistent()
        self.apply.accept(apply)
        return tuple(apply)
