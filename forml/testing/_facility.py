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

from forml import flow, io, project
from forml.conf.parsed import provider as provcfg
from forml.io import dsl, layout
from forml.runtime import facility
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

    def __init__(self, scenario: _spec.Scenario.Input, **kwargs):
        super().__init__(**kwargs)
        self._scenario: _spec.Scenario.Input = scenario

    # pylint: disable=unused-argument
    @classmethod
    def reader(
        cls, sources: typing.Mapping[dsl.Source, None], features: typing.Mapping[dsl.Feature, typing.Any], **kwargs
    ) -> typing.Callable[[dsl.Query], typing.Sequence[typing.Sequence[typing.Any]]]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader)."""

        def read(query: dsl.Query) -> typing.Any:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns:
                Data.
            """
            return features[DataSet.label] if DataSet.label in query.features else features[DataSet.feature]

        return read

    @classmethod
    def slicer(
        cls, schema: typing.Sequence[dsl.Feature], features: typing.Mapping[dsl.Feature, typing.Any]
    ) -> typing.Callable[[layout.ColumnMajor, typing.Union[slice, int]], layout.ColumnMajor]:
        """Return the slicer instance of this feed, that is able to split the loaded dataset column-wise."""
        return lambda c, s: c[s][0]

    @property
    def sources(self) -> typing.Mapping[dsl.Source, None]:
        """The explicit sources mapping implemented by this feed to be used by the query parser."""
        return {DataSet: None}

    @property
    def features(self) -> typing.Mapping[dsl.Feature, typing.Any]:
        """The explicit features mapping implemented by this feed to be used by the query parser."""
        return {DataSet.label: (self._scenario.train, [self._scenario.label]), DataSet.feature: self._scenario.apply}


class Launcher:
    """Test runner is a minimal forml pipeline wrapping the tested operator."""

    class Initializer(flow.Visitor):
        """Visitor that tries to instantiate each node in attempt to validate it."""

        def __init__(self):
            self._gids: set[uuid.UUID] = set()

        def visit_node(self, node: flow.Worker) -> None:
            if isinstance(node, flow.Worker) and node.gid not in self._gids:
                self._gids.add(node.gid)
                node.spec()

    def __init__(self, params: _spec.Scenario.Params, scenario: _spec.Scenario.Input, runner: provcfg.Runner):
        self._params: _spec.Scenario.Params = params
        self._source: project.Source = project.Source.query(DataSet.select(DataSet.feature), DataSet.label)
        self._feed: Feed = Feed(scenario)
        self._runner: provcfg.Runner = runner

    def __call__(self, operator: type[flow.Operator]) -> facility.Virtual.Builder:
        instance = operator(*self._params.args, **self._params.kwargs)
        initializer = self.Initializer()
        segment = instance.expand()
        segment.apply.accept(initializer)
        segment.train.accept(initializer)
        segment.label.accept(initializer)
        return self._source.bind(instance).launcher(self._runner, [self._feed])
