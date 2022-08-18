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
import collections
import datetime
import multiprocessing
import pathlib
import typing
import uuid

import cloudpickle
import pytest

from forml import application as appmod
from forml import flow, io
from forml import project as prjmod
from forml.io import asset, dsl, layout
from forml.pipeline import wrap

from . import helloworld
from .helloworld import application as helloworld_descriptor


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
            raise RuntimeError('Actor not trained')
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


@wrap.Actor.train
def decorated_actor(
    state: typing.Optional[typing.Sequence],
    features: layout.RowMajor,
    labels: layout.Array,
    **params,  # pylint: disable=unused-argument
) -> typing.Sequence:
    """Train part of a stateful actor implemented as a decorated function."""
    state = list(state or [])
    state.append((tuple(tuple(r) for r in features), tuple(labels)))
    return state


@decorated_actor.apply
def decorated_actor(state: typing.Sequence, features: layout.RowMajor, **params) -> layout.RowMajor:
    """Apply part of a stateful actor implemented as a decorated function."""
    model = hash(tuple(state)) ^ hash(tuple(sorted(params.items())))
    return tuple(hash(tuple(f)) ^ model for f in features)


def train_decorator(actor, *args, **kwargs):
    """Wrapping decorator for the train method."""
    return actor.train(*args, **kwargs)


@pytest.fixture(
    scope='session',
    params=(NativeActor, decorated_actor, wrap.Actor.type(WrappedActor, apply='predict', train=train_decorator)),
)
def actor_type(request) -> type[flow.Actor]:
    """Stateful actor fixture."""
    return request.param


@pytest.fixture(scope='session')
def hyperparams() -> typing.Mapping[str, int]:
    """Hyperparams fixture."""
    return dict(a=1, b=2)


@pytest.fixture(scope='session')
def actor_builder(
    actor_type: type[flow.Actor], hyperparams: typing.Mapping[str, int]
) -> flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]:
    """Task builder fixture."""
    return actor_type.builder(**hyperparams)


@pytest.fixture(scope='session')
def trainset() -> layout.RowMajor:
    """Trainset fixture."""
    return helloworld.TRAINSET


@pytest.fixture(scope='session')
def trainset_features() -> layout.RowMajor:
    """Trainset features fixture."""
    return helloworld.TRAINSET_FEATURES


@pytest.fixture(scope='session')
def trainset_labels() -> layout.Array:
    """Trainset labels fixture."""
    return helloworld.TRAINSET_LABELS


@pytest.fixture(scope='session')
def testset() -> layout.RowMajor:
    """Testset fixture."""
    return helloworld.TESTSET


@pytest.fixture(scope='session')
def actor_state(
    actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    trainset_features: layout.RowMajor,
    trainset_labels: layout.Array,
) -> bytes:
    """Actor state fixture."""
    actor = actor_builder()
    actor.train(trainset_features, trainset_labels)
    return actor.get_state()


@pytest.fixture(scope='session')
def actor_prediction(
    actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    actor_state: bytes,
    testset: layout.RowMajor,
) -> layout.RowMajor:
    """Prediction result fixture."""
    actor = actor_builder()
    actor.set_state(actor_state)
    return actor.apply(testset)


@pytest.fixture(scope='session')
def project_package() -> prjmod.Package:
    """Test project package fixture."""
    return helloworld.PACKAGE


@pytest.fixture(scope='session')
def project_path(project_package: prjmod.Package) -> pathlib.Path:
    """Test project path."""
    return project_package.path


@pytest.fixture(scope='session')
def project_manifest(project_package: prjmod.Package) -> prjmod.Manifest:
    """Test project manifest fixture."""
    return project_package.manifest


@pytest.fixture(scope='session')
def project_artifact(project_package: prjmod.Package, project_path: str) -> prjmod.Artifact:
    """Test project artifact fixture."""
    return project_package.install(project_path)


@pytest.fixture(scope='session')
def project_components(project_artifact: prjmod.Artifact) -> prjmod.Components:
    """Test project components fixture."""
    return project_artifact.components


@pytest.fixture(scope='session')
def project_name(project_manifest: prjmod.Manifest) -> asset.Project.Key:
    """Test project name fixture."""
    return project_manifest.name


@pytest.fixture(scope='session')
def project_release(project_manifest: prjmod.Manifest) -> asset.Release.Key:
    """Test project release fixture."""
    return project_manifest.version


@pytest.fixture(scope='session')
def valid_generation() -> asset.Generation.Key:
    """Generation fixture."""
    return asset.Generation.Key(1)


@pytest.fixture(scope='function')
def stateful_nodes() -> typing.Sequence[uuid.UUID]:
    """Helloworld project stateful nodes GID fixtures."""
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16)


@pytest.fixture(scope='function')
def generation_states(stateful_nodes: typing.Sequence[uuid.UUID]) -> typing.Mapping[uuid.UUID, bytes]:
    """Hellworld projects stateful nodes state IDs to state values mapping fixture."""
    return collections.OrderedDict((n, cloudpickle.dumps(i)) for i, n in enumerate(stateful_nodes, start=1))


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
    project_package: prjmod.Package,
) -> asset.Registry:
    """Registry fixture (multiprocess/thread safe)."""
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
        yield helloworld.Registry(content, unbound, lock)


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
def source_query(project_components: prjmod.Components) -> dsl.Query:
    """Query fixture."""
    return project_components.source.extract.train


@pytest.fixture(scope='session')
def testset_entry(testset: layout.RowMajor, source_query: dsl.Query) -> layout.Entry:
    """Entry fixture."""
    return layout.Entry(source_query.schema, layout.Dense.from_rows(testset))


@pytest.fixture(scope='session')
def generation_prediction() -> layout.Array:
    """Stateful prediction fixture."""
    return helloworld.GENERATION_PREDICTION


@pytest.fixture(scope='session')
def testset_outcome(generation_prediction: layout.Array) -> layout.Outcome:
    """Outcome fixture."""
    return layout.Outcome(dsl.Schema.from_fields(dsl.Field(dsl.Integer(), 'prediction')), generation_prediction)


@pytest.fixture(scope='session')
def person_table() -> dsl.Table:
    """Base table fixture."""
    return helloworld.Person


@pytest.fixture(scope='session')
def student_table() -> dsl.Table:
    """Extended table fixture."""
    return helloworld.Student


@pytest.fixture(scope='session')
def school_table() -> dsl.Table:
    """School table fixture."""
    return helloworld.School


@pytest.fixture(scope='session')
def feed_type() -> type[io.Feed]:
    """Dummy feed fixture."""
    return helloworld.Feed


@pytest.fixture(scope='session')
def feed_instance(feed_type: type[io.Feed]) -> io.Feed:
    """Feed instance fixture."""
    return feed_type(identity='test')


@pytest.fixture(scope='session')
def feed_reference(feed_type: type[io.Feed]) -> str:
    """Feed instance reference fixture."""
    return f'{feed_type.__module__}:{feed_type.__qualname__}'


@pytest.fixture(scope='session')
def sink_reference() -> str:
    """Dummy sink reference fixture."""
    return 'dummy'


@pytest.fixture(scope='session')
def sink_type(sink_reference: str) -> type[io.Sink]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Sink(io.Sink, alias=sink_reference):
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


@pytest.fixture(scope='session')
def descriptor_handle() -> appmod.Descriptor.Handle:
    """Application descriptor handle fixture."""
    return appmod.Descriptor.Handle(helloworld_descriptor.__file__)


@pytest.fixture(scope='session')
def descriptor(descriptor_handle: appmod.Descriptor.Handle) -> appmod.Descriptor:
    """Application descriptor fixture."""
    return descriptor_handle.descriptor


@pytest.fixture(scope='session')
def application(descriptor: appmod.Descriptor) -> str:
    """Application name fixture."""
    return descriptor.name


@pytest.fixture(scope='function')
def inventory(descriptor: appmod.Descriptor) -> asset.Inventory:
    """Inventory fixture."""
    return helloworld.Inventory([descriptor])


@pytest.fixture(scope='session')
def testset_request(descriptor: appmod.Descriptor, testset_entry: layout.Entry) -> layout.Request:
    """Request fixture."""
    as_outcome = layout.Outcome(testset_entry.schema, testset_entry.data.to_rows())
    as_response = descriptor.respond(as_outcome, [layout.Encoding('*/*')], None)
    return layout.Request(as_response.payload, as_response.encoding)
