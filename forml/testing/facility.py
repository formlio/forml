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

from forml.conf.parsed import provider as provcfg
from forml.flow.graph import node as nodemod
from forml.flow.graph import view
from forml.flow.pipeline import topology
from forml.io import payload, feed
from forml.io.dsl import struct
from forml.io.dsl.struct import series, frame, kind
from forml.project import component
from forml.runtime import launcher
from forml.testing import spec

LOGGER = logging.getLogger(__name__)


class DataSet(struct.Schema):
    """Testing schema.

    The actual fields are irrelevant.
    """

    feature: struct.Field = struct.Field(kind.Integer())
    label: struct.Field = struct.Field(kind.Float())


class Feed(feed.Provider[None, typing.Any], alias='testing'):
    """Special feed to input the test cases."""

    def __init__(self, scenario: spec.Scenario.Input, **kwargs):
        super().__init__(**kwargs)
        self._scenario: spec.Scenario.Input = scenario

    # pylint: disable=unused-argument
    @classmethod
    def reader(
        cls, sources: typing.Mapping[frame.Source, None], columns: typing.Mapping[series.Column, typing.Any], **kwargs
    ) -> typing.Callable[[frame.Query], typing.Sequence[typing.Sequence[typing.Any]]]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader)."""

        def read(query: frame.Query) -> typing.Any:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns:
                Data.
            """
            return columns[DataSet.label] if DataSet.label in query.columns else columns[DataSet.feature]

        return read

    @classmethod
    def slicer(
        cls, schema: typing.Sequence[series.Column], columns: typing.Mapping[series.Column, typing.Any]
    ) -> typing.Callable[[payload.ColumnMajor, typing.Union[slice, int]], payload.ColumnMajor]:
        """Return the slicer instance of this feed, that is able to split the loaded dataset column-wise."""
        return lambda c, s: c[s][0]

    @property
    def sources(self) -> typing.Mapping[frame.Source, None]:
        """The explicit sources mapping implemented by this feed to be used by the query parser."""
        return {DataSet: None}

    @property
    def columns(self) -> typing.Mapping[series.Column, typing.Any]:
        """The explicit columns mapping implemented by this feed to be used by the query parser."""
        return {DataSet.label: (self._scenario.train, [self._scenario.label]), DataSet.feature: self._scenario.apply}


class Launcher:
    """Test runner is a minimal forml pipeline wrapping the tested operator."""

    class Initializer(view.Visitor):
        """Visitor that tries to instantiate each node in attempt to validate it."""

        def __init__(self):
            self._gids: typing.Set[uuid.UUID] = set()

        def visit_node(self, node: nodemod.Worker) -> None:
            if isinstance(node, nodemod.Worker) and node.gid not in self._gids:
                self._gids.add(node.gid)
                node.spec()

    def __init__(self, params: spec.Scenario.Params, scenario: spec.Scenario.Input, runner: provcfg.Runner):
        self._params: spec.Scenario.Params = params
        self._source: component.Source = component.Source.query(DataSet.select(DataSet.feature), DataSet.label)
        self._feed: Feed = Feed(scenario)
        self._runner: provcfg.Runner = runner

    def __call__(self, operator: typing.Type[topology.Operator]) -> launcher.Virtual.Builder:
        instance = operator(*self._params.args, **self._params.kwargs)
        initializer = self.Initializer()
        segment = instance.expand()
        segment.apply.accept(initializer)
        segment.train.accept(initializer)
        segment.label.accept(initializer)
        return self._source.bind(instance).launcher(self._runner, [self._feed])
