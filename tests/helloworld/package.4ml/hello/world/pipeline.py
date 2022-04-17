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
Dummy project pipeline.
"""
import struct
import typing

from forml import flow, project
from forml.io import layout
from forml.pipeline import topology


@topology.Function.actor
def split(rows: layout.RowMajor) -> tuple[layout.RowMajor, layout.RowMajor]:
    """Actor splitting set of rows into two."""
    mid = len(rows) // 2
    return rows[:mid], rows[mid:]


@topology.Function.actor
def merge(left: typing.Sequence[int], right: typing.Sequence[int]) -> typing.Sequence[int]:
    """Actor merging two vectors as positional sum."""
    return [a + b for a, b in zip(left, right)]


@topology.Mapper.operator
@topology.Function.actor
def select(rows: typing.Sequence[tuple[str, str, int]]) -> typing.Sequence[int]:
    """Operator for selecting just the 3rd column."""
    return [r[2] for r in rows]


@topology.Consumer.operator
class HelloWorld(flow.Actor[typing.Sequence[int], typing.Sequence[int], typing.Sequence[int]]):
    """Stateful transformer."""

    FMT = '!Q'

    def __init__(self):
        self._magic = 1

    def train(self, features: typing.Sequence[int], labels: typing.Sequence[int]):
        """Impute missing values using the median for numeric columns and the most common value for string columns."""
        self._magic += sum(labels)

    def apply(self, rows: typing.Sequence[int]) -> typing.Sequence[int]:
        """Filling the NaNs."""
        return [r * self._magic for r in rows]

    def get_state(self) -> bytes:
        return struct.pack(self.FMT, self._magic)

    def set_state(self, state: bytes) -> None:
        self._magic = struct.unpack(self.FMT, state)[0]


class Branches(flow.Operator):
    """Operator that splits the flow and adds the left/right stateful branches and the merges them back together."""

    def __init__(self, left: flow.Composable, right: flow.Composable):
        self._left: flow.Composable = left
        self._right: flow.Composable = right

    def compose(self, left: flow.Composable) -> flow.Trunk:
        head: flow.Trunk = flow.Trunk()
        feature_splitter: flow.Worker = flow.Worker(split.spec(), 1, 2)
        feature_splitter[0].subscribe(head.train.publisher)
        label_splitter: flow.Worker = flow.Worker(split.spec(), 1, 2)
        label_splitter[0].subscribe(head.label.publisher)
        merger: flow.Worker = flow.Worker(merge.spec(), 2, 1)
        for fid, pipeline_branch in enumerate((self._left.expand(), self._right.expand())):
            fold_train: flow.Trunk = left.expand()
            fold_train.train.subscribe(feature_splitter[fid])
            fold_train.label.subscribe(label_splitter[fid])

            fold_apply: flow.Path = fold_train.apply.copy()
            fold_apply.subscribe(head.apply)

            pipeline_branch.train.subscribe(fold_train.train)
            pipeline_branch.label.subscribe(fold_train.label)
            pipeline_branch.apply.subscribe(fold_apply)
            merger[fid].subscribe(pipeline_branch.apply.publisher)
        return head.use(apply=head.apply.extend(tail=merger))


INSTANCE = select() >> Branches(HelloWorld(), HelloWorld())  # pylint: disable=no-value-for-parameter
project.setup(INSTANCE)
