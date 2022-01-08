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
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use
import collections
import datetime
import multiprocessing
import pathlib
import struct
import typing
import uuid

import pytest

from forml import flow, io
from forml import project as prj
from forml.io import dsl, layout
from forml.io.dsl import parser as parsmod
from forml.lib.pipeline import topology
from forml.runtime import asset

from . import helloworld_schema


class WrappedActor:
    """Actor to-be mockup."""

    def __init__(self, **params):
        self._model = []
        self._params = params
        self._hash: int = 0

    def train(self, features: layout.RowMajor, labels: layout.Array) -> None:
        """Train to-be handler."""
        self._model.append((tuple(tuple(r) for r in features), tuple(labels)))

    def predict(self, features: layout.RowMajor) -> layout.RowMajor:
        """Apply to-be handler."""
        if not self._model:
            raise ValueError('Not Fitted')
        model = hash(tuple(self._model)) ^ hash(tuple(sorted(self._params.items())))
        return tuple(hash(tuple(f)) ^ model for f in features)

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        """Get hyper-parameters of this actor."""
        return self._params

    def set_params(self, **params: typing.Any):
        """Set hyper-parameters of this actor."""
        self._params.update(params)


class NativeActor(WrappedActor, flow.Actor[layout.RowMajor, None, layout.RowMajor]):
    """Actor implementation."""

    def apply(self, *features: layout.RowMajor) -> layout.RowMajor:
        """Native apply method."""
        return self.predict(features[0])


def train_decorator(actor, *args, **kwargs):
    """Wrapping decorator for the train method."""
    return actor.train(*args, **kwargs)


@pytest.fixture(
    scope='session',
    params=(NativeActor, topology.Class.actor(WrappedActor, apply='predict', train=train_decorator)),
)
def actor_type(request) -> type[flow.Actor]:
    """Stateful actor fixture."""
    return request.param


@pytest.fixture(scope='session')
def hyperparams() -> typing.Mapping[str, int]:
    """Hyperparams fixture."""
    return dict(a=1, b=2)


@pytest.fixture(scope='session')
def actor_spec(
    actor_type: type[flow.Actor], hyperparams: typing.Mapping[str, int]
) -> flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor]:
    """Task spec fixture."""
    return flow.Spec(actor_type, **hyperparams)


@pytest.fixture(scope='session')
def trainset() -> layout.RowMajor:
    """Trainset fixture."""
    return (
        ('smith', 'oxford', 1, 1),
        ('black', 'cambridge', 2, 1),
        ('harris', 'stanford', 3, 3),
    )


@pytest.fixture(scope='session')
def trainset_features(trainset: layout.RowMajor) -> layout.RowMajor:
    """Trainset features fixture."""
    return layout.Dense.from_rows(trainset).take_columns([0, 1, 2]).to_rows()


@pytest.fixture(scope='session')
def trainset_labels(trainset: layout.RowMajor) -> layout.Array:
    """Trainset labels fixture."""
    return layout.Dense.from_rows(trainset).to_columns()[-1]


@pytest.fixture(scope='session')
def testset(trainset_features: layout.RowMajor) -> layout.RowMajor:
    """Testset fixture."""
    return trainset_features


@pytest.fixture(scope='session')
def actor_state(
    actor_spec: flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor],
    trainset_features: layout.RowMajor,
    trainset_labels: layout.Array,
) -> bytes:
    """Actor state fixture."""
    actor = actor_spec()
    actor.train(trainset_features, trainset_labels)
    return actor.get_state()


@pytest.fixture(scope='session')
def actor_prediction(
    actor_spec: flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor], actor_state: bytes, testset: layout.RowMajor
) -> layout.RowMajor:
    """Prediction result fixture."""
    actor = actor_spec()
    actor.set_state(actor_state)
    return actor.apply(testset)


@pytest.fixture(scope='session')
def project_path() -> pathlib.Path:
    """Test project path."""
    return pathlib.Path(__file__).parent / 'helloworld'


@pytest.fixture(scope='session')
def project_package(project_path: pathlib.Path) -> prj.Package:
    """Test project package fixture."""
    return prj.Package(project_path)


@pytest.fixture(scope='session')
def project_manifest(project_package: prj.Package) -> prj.Manifest:
    """Test project manifest fixture."""
    return project_package.manifest


@pytest.fixture(scope='session')
def project_artifact(project_package: prj.Package, project_path: str) -> prj.Artifact:
    """Test project artifact fixture."""
    return project_package.install(project_path)


@pytest.fixture(scope='session')
def project_components(project_artifact: prj.Artifact) -> prj.Components:
    """Test project components fixture."""
    return project_artifact.components


@pytest.fixture(scope='session')
def project_name(project_manifest: prj.Manifest) -> asset.Project.Key:
    """Test project name fixture."""
    return project_manifest.name


@pytest.fixture(scope='session')
def project_release(project_manifest: prj.Manifest) -> asset.Release.Key:
    """Test project release fixture."""
    return project_manifest.version


@pytest.fixture(scope='session')
def valid_generation() -> asset.Generation.Key:
    """Generation fixture."""
    return asset.Generation.Key(1)


@pytest.fixture(scope='function')
def stateful_nodes() -> typing.Sequence[uuid.UUID]:
    """Persistent nodes GID fixture."""
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16)


@pytest.fixture(scope='function')
def generation_states(stateful_nodes: typing.Sequence[uuid.UUID]) -> typing.Mapping[uuid.UUID, bytes]:
    """State IDs to state values mapping fixture."""
    return collections.OrderedDict((n, struct.pack('!Q', i)) for i, n in enumerate(stateful_nodes, start=1))


@pytest.fixture(scope='function')
def generation_tag(generation_states: typing.Mapping[uuid.UUID, bytes]) -> asset.Tag:
    """Tag fixture."""
    return asset.Tag(
        training=asset.Tag.Training(datetime.datetime(2019, 4, 1), datetime.datetime(2019, 1, 2)),
        tuning=asset.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
        states=generation_states.keys(),
    )


@pytest.fixture(scope='session')
def empty_release(project_release: asset.Release.Key) -> asset.Release.Key:
    """Release fixture."""
    return asset.Release.Key(f'{project_release}.1')


@pytest.fixture(scope='function')
def registry(
    project_name: asset.Project.Key,
    project_release: asset.Release.Key,
    empty_release: asset.Release.Key,
    valid_generation: asset.Generation.Key,
    generation_tag: asset.Tag,
    generation_states: typing.Mapping[uuid.UUID, bytes],
    project_package: prj.Package,
) -> asset.Registry:
    """Registry fixture (multiprocess/thread safe)."""

    class Registry(asset.Registry):
        """Fixture registry implementation."""

        def projects(self) -> typing.Iterable[str]:
            with lock:
                return content.keys()

        def releases(self, project: asset.Project.Key) -> typing.Iterable[asset.Release.Key]:
            with lock:
                return content[project].keys()

        def generations(
            self, project: asset.Project.Key, release: asset.Release.Key
        ) -> typing.Iterable[asset.Generation.Key]:
            try:
                with lock:
                    return content[project][release][1].keys()
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid release ({release})') from err

        def pull(self, project: asset.Project.Key, release: asset.Release.Key) -> prj.Package:
            with lock:
                return content[project][release][0]

        def push(self, package: prj.Package) -> None:
            raise NotImplementedError()

        def read(
            self,
            project: asset.Project.Key,
            release: asset.Release.Key,
            generation: asset.Generation.Key,
            sid: uuid.UUID,
        ) -> bytes:
            with lock:
                if sid not in content[project][release][1][generation][0].states:
                    raise asset.Level.Invalid(f'Invalid state id ({sid})')
                idx = content[project][release][1][generation][0].states.index(sid)
                return content[project][release][1][generation][1][idx]

        def write(self, project: asset.Project.Key, release: asset.Release.Key, sid: uuid.UUID, state: bytes) -> None:
            with lock:
                unbound[sid] = state

        def open(
            self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
        ) -> asset.Tag:
            try:
                with lock:
                    return content[project][release][1][generation][0]
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid generation ({release}.{generation})') from err

        def close(
            self,
            project: asset.Project.Key,
            release: asset.Release.Key,
            generation: asset.Generation.Key,
            tag: asset.Tag,
        ) -> None:
            with lock:
                assert set(tag.states).issubset(unbound.keys())
                assert (
                    project in content
                    and release in content[project]
                    and generation not in content[project][release][1]
                )
                content[project][release][1][generation] = (tag, tuple(unbound[k] for k in tag.states))
                unbound.clear()

    with multiprocessing.Manager() as manager:
        lock = manager.RLock()
        content = manager.dict(
            {
                project_name: {
                    project_release: (
                        project_package,
                        {valid_generation: (generation_tag, tuple(generation_states.values()))},
                    ),
                    empty_release: (project_package, {}),
                }
            }
        )
        unbound: dict[uuid.UUID, bytes] = manager.dict()
        yield Registry()


@pytest.fixture(scope='function')
def directory(registry: asset.Registry) -> asset.Directory:
    """Directory root fixture."""
    return asset.Directory(registry)


@pytest.fixture(scope='function')
def valid_instance(
    project_name: asset.Project.Key,
    project_release: asset.Release.Key,
    valid_generation: asset.Generation.Key,
    directory: asset.Directory,
) -> asset.Instance:
    """Asset instance fixture."""
    return asset.Instance(project_name, project_release, valid_generation, directory)


@pytest.fixture(scope='session')
def source_query(project_components) -> dsl.Query:
    """Query fixture."""
    return project_components.source.extract.train


@pytest.fixture(scope='session')
def person_table() -> dsl.Table:
    """Base table fixture."""
    return helloworld_schema.Person


@pytest.fixture(scope='session')
def student_table() -> dsl.Table:
    """Extended table fixture."""
    return helloworld_schema.Student


@pytest.fixture(scope='session')
def school_table() -> dsl.Table:
    """School table fixture."""
    return helloworld_schema.School


@pytest.fixture(scope='session')
def io_reference() -> str:
    """Dummy feed/sink reference fixture."""
    return 'dummy'


@pytest.fixture(scope='session')
def feed_type(
    io_reference: str,
    person_table: dsl.Table,
    student_table: dsl.Table,
    school_table: dsl.Table,
    trainset: layout.RowMajor,
    testset: layout.RowMajor,  # pylint: disable=unused-argument
) -> type[io.Feed]:
    """Dummy feed fixture."""

    class Feed(io.Feed[str, str], alias=io_reference):
        """Dummy feed for unit-testing purposes."""

        class Reader(io.Feed.Reader[str, str, layout.RowMajor]):
            """Dummy reader that returns either the trainset or testset fixtures."""

            class Parser(parsmod.Visitor[str, str]):
                """Dummy parser that returns string keyword of `trainset` or `testset` depending on the number
                of projected columns."""

                resolve_feature = (
                    generate_alias
                ) = generate_expression = generate_join = generate_literal = generate_set = lambda *_: ''
                generate_reference = lambda *_: ('', '')

                def generate_element(self, origin: str, element: str) -> str:
                    return f'{origin}-{element}'

                def generate_query(
                    self,
                    source: str,
                    features: typing.Sequence[str],
                    where: typing.Optional[str],
                    groupby: typing.Sequence[str],
                    having: typing.Optional[str],
                    orderby: typing.Sequence[tuple[str, dsl.Ordering.Direction]],
                    rows: typing.Optional[dsl.Rows],
                ) -> str:
                    return 'testset' if len(features) == len(testset[0]) else 'trainset'

            @classmethod
            def parser(
                cls,
                sources: typing.Mapping[dsl.Source, parsmod.Source],
                features: typing.Mapping[dsl.Feature, parsmod.Feature],
            ) -> parsmod.Visitor:
                return cls.Parser(sources, features)  # pylint: disable=abstract-class-instantiated

            @classmethod
            def read(cls, statement: str, **kwargs: typing.Any) -> layout.RowMajor:
                return testset if statement == 'testset' else trainset

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

        @property
        def sources(self) -> typing.Mapping[dsl.Source, parsmod.Source]:
            """Abstract method implementation."""
            return {
                student_table.join(person_table, student_table.surname == person_table.surname).source: 'pupil',
                person_table: 'person',
                student_table: 'student',
                school_table: 'school',
            }

    return Feed


@pytest.fixture(scope='session')
def feed_instance(feed_type: type[io.Feed]) -> io.Feed:
    """Feed instance fixture"""
    return feed_type(identity='test')


@pytest.fixture(scope='session')
def sink_type(io_reference: str) -> type[io.Sink]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Sink(io.Sink, alias=io_reference):
        """Dummy sink for unit-testing purposes."""

        class Writer(io.Sink.Writer[layout.RowMajor]):
            """Dummy black-hole sink writer."""

            @classmethod
            def write(cls, data: layout.RowMajor, queue: typing.Optional[multiprocessing.Queue] = None) -> None:
                if queue:
                    queue.put(data)

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

    return Sink


@pytest.fixture(scope='function')
def sink_output() -> multiprocessing.Queue:
    """Sink output queue."""
    with multiprocessing.Manager() as manager:
        yield manager.Queue()


@pytest.fixture(scope='function')
def sink_instance(sink_type: type[io.Sink], sink_output: multiprocessing.Queue) -> io.Sink:
    """Sink instance fixture"""
    return sink_type(identity='test', queue=sink_output)
