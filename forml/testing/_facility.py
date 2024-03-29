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
Testing facility.
"""
import logging
import typing
import uuid

from forml import flow, io, project, runtime, setup
from forml.io import dsl
from forml.testing import _spec

LOGGER = logging.getLogger(__name__)


class DataSet(dsl.Schema):
    """Testing schema.

    The actual fields are irrelevant.
    """

    feature: dsl.Field = dsl.Field(dsl.Integer())
    label: dsl.Field = dsl.Field(dsl.Float())


class Feed(io.Feed[None, typing.Any], alias='testing'):
    """Special feed to input the test cases."""

    class Operator(flow.Operator):
        """Loading operator that just returns the data provided for the scenario."""

        class Apply(flow.Actor[None, None, typing.Any]):
            """Actor returning the provided testset."""

            def __init__(self, features: typing.Any):
                self._features: typing.Any = features

            def apply(self) -> typing.Any:
                return self._features

        class Train(flow.Actor[None, None, tuple[typing.Any, typing.Any]]):
            """Actor returning the provided trainset."""

            def __init__(self, features: typing.Any, labels: typing.Any):
                self._features: typing.Any = features
                self._labels: typing.Any = labels

            def apply(self) -> tuple[typing.Any, typing.Any]:
                return self._features, self._labels

        def __init__(self, scenario: _spec.Scenario.Input):
            self._testset: flow.Builder[Feed.Operator.Apply] = self.Apply.builder(scenario.apply)
            self._trainset: flow.Builder[Feed.Operator.Train] = self.Train.builder(scenario.train, scenario.label)

        def compose(self, scope: flow.Composable) -> flow.Trunk:
            """Compose the source segment trunk.

            Returns:
                Source segment trunk.
            """
            testset = flow.Worker(self._testset, 0, 1)
            trainset = flow.Worker(self._trainset, 0, 2)
            train = flow.Future()
            train[0].subscribe(trainset[0])
            label = flow.Future()
            label[0].subscribe(trainset[1])
            return flow.Trunk(testset, flow.Segment(trainset, train), flow.Segment(trainset, label))

    def __init__(self, scenario: _spec.Scenario.Input, **kwargs):
        super().__init__(**kwargs)
        self._operator: flow.Operator = self.Operator(scenario)

    def load(
        self,
        extract: 'project.Source.Extract',
        lower: typing.Optional['dsl.Native'] = None,
        upper: typing.Optional['dsl.Native'] = None,
    ) -> flow.Composable:
        return self._operator

    @property
    def sources(self) -> typing.Mapping[dsl.Source, None]:
        """The explicit sources mapping implemented by this feed to be used by the query parser."""
        return {DataSet: None}


class Launcher:
    """Test runner is a minimal forml pipeline wrapping the tested operator."""

    class Action:
        """Customized launcher actions exposed to testing routines."""

        def __init__(self, handler: runtime.Virtual.Handler):
            self._handler: runtime.Virtual.Handler = handler

        def apply(self) -> typing.Any:
            """Normal apply mode."""
            return self._handler.apply()

        def train_call(self) -> None:
            """Normal train mode (no output captured/returned)."""
            self._handler.train()

        def train_return(self) -> tuple[flow.Features, flow.Labels]:
            """Extended train mode that's capturing and returning the features+labels output."""
            result = self._handler.train()
            return result.features, result.labels

    class Initializer(flow.Visitor):
        """Visitor that tries to instantiate each node in attempt to validate it."""

        def __init__(self):
            self._gids: set[uuid.UUID] = set()

        def visit_node(self, node: flow.Worker) -> None:
            if isinstance(node, flow.Worker) and node.gid not in self._gids:
                self._gids.add(node.gid)
                node.builder()

    def __init__(self, params: _spec.Scenario.Params, scenario: _spec.Scenario.Input, runner: setup.Runner):
        self._params: _spec.Scenario.Params = params
        self._source: project.Source = project.Source.query(DataSet.select(DataSet.feature), DataSet.label)
        self._feed: Feed = Feed(scenario)
        self._runner: setup.Runner = runner

    def __call__(self, operator: type[flow.Operator]) -> 'Launcher.Action':
        instance = operator(*self._params.args, **self._params.kwargs)
        initializer = self.Initializer()
        trunk = instance.expand()
        trunk.apply.accept(initializer)
        trunk.train.accept(initializer)
        trunk.label.accept(initializer)

        return self.Action(self._source.bind(instance).launcher(self._runner, [self._feed]))
