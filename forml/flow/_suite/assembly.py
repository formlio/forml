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

from .._graph import node as nodemod
from .._graph import span
from . import clean

if typing.TYPE_CHECKING:
    from forml import flow


class Trunk(collections.namedtuple('Trunk', 'apply, train, label')):
    """Structure for holding three main related paths."""

    apply: 'flow.Path'
    train: 'flow.Path'
    label: 'flow.Path'

    def __new__(
        cls,
        apply: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        train: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        label: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
    ):
        def init(mode: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']]) -> span.Path:
            """Apply default cleaning to the mode segment.

            Args:
                mode: Mode segment provided either as a path or just a node.

            Returns:
                Cleaned mode path.
            """
            if not mode:
                mode = nodemod.Future()
            if isinstance(mode, nodemod.Atomic):
                mode = span.Path(mode)
            return mode

        return super().__new__(cls, init(apply), init(train), init(label))

    def extend(
        self,
        apply: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        train: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        label: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
    ) -> 'flow.Trunk':
        """Helper for creating new Trunk with specified paths extended by provided values.

        Args:
            apply: Optional path to be connected to apply segment.
            train: Optional path to be connected to train segment.
            label: Optional path to be connected to label segment.

        Returns:
            New Track instance.
        """
        return self.__class__(
            self.apply.extend(apply) if apply else self.apply,
            self.train.extend(train) if train else self.train,
            self.label.extend(label) if label else self.label,
        )

    def use(
        self,
        apply: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        train: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
        label: typing.Optional[typing.Union['flow.Path', 'flow.Atomic']] = None,
    ) -> 'flow.Trunk':
        """Helper for creating new Trunk with specified paths replaced by provided values.

        Args:
            apply: Optional path to be used as apply segment.
            train: Optional path to be used as train segment.
            label: Optional path to be used as label segment.

        Returns:
            New Track instance.
        """
        return self.__class__(apply or self.apply, train or self.train, label or self.label)


class Composition(collections.namedtuple('Composition', 'apply, train')):
    """Structure for holding related flow parts of different modes."""

    apply: 'flow.Path'
    train: 'flow.Path'

    class Persistent(span.Visitor, typing.Iterable[uuid.UUID]):
        """Visitor that cumulates gids of persistent nodes."""

        def __init__(self):
            self._gids: list[uuid.UUID] = []

        def __iter__(self) -> typing.Iterator[uuid.UUID]:
            return iter(self._gids)

        def visit_node(self, node: nodemod.Worker) -> None:
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

        The states used within the apply path are expected to be subset of the states used in the train path (since not
        all the stateful workers engaged during training are necessarily used during apply and hence don't need
        persisting so we can ignore them).

        Returns:
            Set of nodes sharing state between pipeline modes.
        """
        apply = self.Persistent()
        self.apply.accept(apply)
        return tuple(apply)
