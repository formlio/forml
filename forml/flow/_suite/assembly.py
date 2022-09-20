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
        """Helper for creating new Trunk with the specified segments *extended* by the provided instances.

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

    class Builder(tuple):
        """Composition builder."""

        def __new__(cls, *blocks: 'flow.Composable'):
            return super().__new__(cls, blocks)

        def via(self, block: 'flow.Composable') -> 'flow.Composition.Builder':
            """Return a new builder with the given block appended.

            Args:
                block: Additional block to be appended at the end of the current chain.

            Returns:
                New builder instance.
            """
            return self.__class__(*self, block)

        def build(self, sink: typing.Optional['flow.Composable'] = None) -> 'flow.Composition':
            """Close the builder with the optional sink.

            Args:
                sink: Optional (stateless) sink closing the builder.

            Returns: Composition instance.
            """
            segments = list(self)
            if sink:
                segments.append(clean.Stateless(sink))
            return Composition(*segments)

    def __new__(cls, /, first: 'flow.Composable', *others: 'flow.Composable'):
        composed = functools.reduce(lambda c, s: c.extend(*s.expand()), others, first.expand())

        apply = composed.apply.extend()
        apply.accept(clean.Validator())
        train = composed.train.extend()
        train.accept(clean.Validator())
        # label = composed.label.extend()
        # label.accept(clean.Validator())
        return super().__new__(cls, apply, train)

    @classmethod
    def builder(
        cls, extract: 'flow.Composable', transform: typing.Optional['flow.Composable'] = None
    ) -> 'flow.Composition.Builder':
        """Create a composition builder using the two source ETL components.

        Args:
            extract: Mandatory (stateless) part of the ETL.
            transform: Optional transform part of the ETL.

        Returns:
            Composition builder.
        """
        loader = clean.Stateless(extract)
        if transform:
            loader >>= transform
        return cls.Builder(loader)

    @functools.cached_property
    def persistent(self) -> typing.Sequence[uuid.UUID]:
        """Get the set of nodes with state that needs to be carried over between the apply/train
        modes.

        The states used within the apply segment are expected to be subset of the states used in
        the train segment (since not all the stateful workers engaged during training are
        necessarily used during apply and hence don't need persisting so we can ignore them).

        Returns:
            Set of nodes sharing state between pipeline modes.
        """
        apply = clean.Stateful()
        self.apply.accept(apply)
        return tuple(apply)
