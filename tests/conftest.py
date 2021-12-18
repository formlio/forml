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
import pathlib
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

    def train(self, features, labels) -> None:
        """Train to-be handler."""
        self._model.append((features, labels))

    def predict(self, features: typing.Sequence[typing.Any]) -> typing.Sequence[typing.Any]:
        """Apply to-be handler."""
        if not self._model:
            raise ValueError('Not Fitted')
        model = hash(tuple(self._model)) ^ hash(tuple(sorted(self._params.items())))
        return tuple(hash(f) ^ model for f in features)

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        """Get hyper-parameters of this actor."""
        return self._params

    def set_params(self, **params: typing.Any):
        """Set hyper-parameters of this actor."""
        self._params.update(params)


class NativeActor(WrappedActor, flow.Actor):
    """Actor implementation."""

    def apply(self, *features: typing.Any) -> typing.Any:
        """Native apply method."""
        return self.predict(features[0])


def train_decorator(actor, *args, **kwargs):
    """Wrapping decorator for the train method."""
    return actor.train(*args, **kwargs)


@pytest.fixture(
    scope='session',
    params=(NativeActor, topology.Class.actor(WrappedActor, apply='predict', train=train_decorator)),
)
def actor(request) -> type[flow.Actor]:
    """Stateful actor fixture."""
    return request.param


@pytest.fixture(scope='session')
def hyperparams() -> typing.Mapping[str, int]:
    """Hyperparams fixture."""
    return dict(a=1, b=2)


@pytest.fixture(scope='session')
def spec(actor: type[flow.Actor], hyperparams):
    """Task spec fixture."""
    return flow.Spec(actor, **hyperparams)


@pytest.fixture(scope='session')
def trainset() -> layout.ColumnMajor:
    """Trainset fixture."""
    return ('smith', 'black', 'harris'), ('oxford', 'cambridge', 'stanford'), (1, 2, 3), (1, 1, 3)


@pytest.fixture(scope='session')
def testset(trainset: layout.ColumnMajor) -> layout.ColumnMajor:
    """Testset fixture."""
    return trainset[:-1]


@pytest.fixture(scope='session')
def state(spec: flow.Spec, trainset: layout.ColumnMajor) -> bytes:
    """Actor state fixture."""
    actor = spec()
    actor.train(trainset[:-1], trainset[-1])
    return actor.get_state()


@pytest.fixture(scope='session')
def prediction(spec: flow.Spec, state: bytes, testset: layout.ColumnMajor) -> layout.ColumnMajor:
    """Prediction result fixture."""
    actor = spec()
    actor.set_state(state)
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
def project_descriptor(project_artifact: prj.Artifact) -> prj.Descriptor:
    """Test project artifact fixture."""
    return project_artifact.descriptor


@pytest.fixture(scope='session')
def project_name(project_manifest: prj.Manifest) -> asset.Project.Key:
    """Test project name fixture."""
    return project_manifest.name


@pytest.fixture(scope='session')
def project_lineage(project_manifest: prj.Manifest) -> asset.Lineage.Key:
    """Test project lineage fixture."""
    return project_manifest.version


@pytest.fixture(scope='session')
def valid_generation() -> asset.Generation.Key:
    """Generation fixture."""
    return asset.Generation.Key(1)


@pytest.fixture(scope='function')
def nodes() -> typing.Sequence[uuid.UUID]:
    """Persistent nodes GID fixture."""
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16), uuid.UUID(bytes=b'\x02' * 16)


@pytest.fixture(scope='function')
def states(nodes) -> typing.Mapping[uuid.UUID, bytes]:
    """State IDs to state values mapping fixture."""
    return collections.OrderedDict((n, n.bytes) for n in nodes)


@pytest.fixture(scope='function')
def tag(states: typing.Mapping[uuid.UUID, bytes]) -> asset.Tag:
    """Tag fixture."""
    return asset.Tag(
        training=asset.Tag.Training(datetime.datetime(2019, 4, 1), 123),
        tuning=asset.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
        states=states.keys(),
    )


@pytest.fixture(scope='session')
def empty_lineage(project_lineage: asset.Lineage.Key) -> asset.Lineage.Key:
    """Lineage fixture."""
    return asset.Lineage.Key(f'{project_lineage}.1')


@pytest.fixture(scope='function')
def registry(
    project_name: asset.Project.Key,
    project_lineage: asset.Lineage.Key,
    empty_lineage: asset.Lineage.Key,
    valid_generation: asset.Generation.Key,
    tag: asset.Tag,
    states: typing.Mapping[uuid.UUID, bytes],
    project_package: prj.Package,
) -> asset.Registry:
    """Registry fixture."""
    content = {
        project_name: {
            project_lineage: (project_package, {valid_generation: (tag, tuple(states.values()))}),
            empty_lineage: (project_package, {}),
        }
    }

    class Registry(asset.Registry):
        """Fixture registry implementation"""

        def projects(self) -> typing.Iterable[str]:
            return content.keys()

        def lineages(self, project: asset.Project.Key) -> typing.Iterable[asset.Lineage.Key]:
            return content[project].keys()

        def generations(
            self, project: asset.Project.Key, lineage: asset.Lineage.Key
        ) -> typing.Iterable[asset.Generation.Key]:
            try:
                return content[project][lineage][1].keys()
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid lineage ({lineage})') from err

        def pull(self, project: asset.Project.Key, lineage: asset.Lineage.Key) -> prj.Package:
            return content[project][lineage][0]

        def push(self, package: prj.Package) -> None:
            raise NotImplementedError()

        def read(
            self,
            project: asset.Project.Key,
            lineage: asset.Lineage.Key,
            generation: asset.Generation.Key,
            sid: uuid.UUID,
        ) -> bytes:
            if sid not in content[project][lineage][1][generation][0].states:
                raise asset.Level.Invalid(f'Invalid state id ({sid})')
            idx = content[project][lineage][1][generation][0].states.index(sid)
            return content[project][lineage][1][generation][1][idx]

        def write(self, project: asset.Project.Key, lineage: asset.Lineage.Key, sid: uuid.UUID, state: bytes) -> None:
            raise NotImplementedError()

        def open(
            self, project: asset.Project.Key, lineage: asset.Lineage.Key, generation: asset.Generation.Key
        ) -> asset.Tag:
            try:
                return content[project][lineage][1][generation][0]
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid generation ({lineage}.{generation})') from err

        def close(
            self,
            project: asset.Project.Key,
            lineage: asset.Lineage.Key,
            generation: asset.Generation.Key,
            tag: asset.Tag,
        ) -> None:
            raise NotImplementedError()

    return Registry()


@pytest.fixture(scope='function')
def directory(registry: asset.Registry) -> asset.Directory:
    """Directory root fixture."""
    return asset.Directory(registry)


@pytest.fixture(scope='function')
def valid_instance(
    project_name: asset.Project.Key,
    project_lineage: asset.Lineage.Key,
    valid_generation: asset.Generation.Key,
    directory: asset.Directory,
) -> asset.Instance:
    """Asset instance fixture."""
    return asset.Instance(project_name, project_lineage, valid_generation, directory)


@pytest.fixture(scope='session')
def query(project_descriptor: prj.Descriptor) -> dsl.Query:
    """Query fixture."""
    return project_descriptor.source.extract.train


@pytest.fixture(scope='session')
def person() -> dsl.Table:
    """Base table fixture."""
    return helloworld_schema.Person


@pytest.fixture(scope='session')
def student() -> dsl.Table:
    """Extended table fixture."""
    return helloworld_schema.Student


@pytest.fixture(scope='session')
def school() -> dsl.Table:
    """School table fixture."""
    return helloworld_schema.School


@pytest.fixture(scope='session')
def reference() -> str:
    """Dummy feed/sink reference fixture"""
    return 'dummy'


@pytest.fixture(scope='session')
def feed(
    reference: str,
    person: dsl.Table,
    student: dsl.Table,
    school: dsl.Table,
    trainset: layout.ColumnMajor,
    testset: layout.ColumnMajor,  # pylint: disable=unused-argument
) -> type[io.Feed]:
    """Dummy feed fixture."""

    class Dummy(io.Feed, alias=reference):
        """Dummy feed for unit-testing purposes."""

        class Reader(io.Feed.Reader):
            """Dummy reader that returns either the trainset or testset fixtures."""

            class Parser(parsmod.Visitor[str, str]):
                """Dummy parser that returns string keyword of `trainset` or `testset` depending on the number
                of projected columns."""

                # pylint: disable=abstract-method
                def __getattr__(self, item: str) -> typing.Callable[..., str]:
                    """Fake implementation of the remaining `generate_*(...)` methods."""
                    return lambda *_: ''

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
                    return 'testset' if len(features) == len(testset) else 'trainset'

            @classmethod
            def parser(
                cls,
                sources: typing.Mapping[dsl.Source, parsmod.Source],
                features: typing.Mapping[dsl.Feature, parsmod.Feature],
            ) -> parsmod.Visitor:
                return cls.Parser(sources, features)  # pylint: disable=abstract-class-instantiated

            @classmethod
            def read(cls, statement: str, **kwargs: typing.Any) -> layout.Native:
                return testset if statement == 'testset' else trainset

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

        @property
        def sources(self) -> typing.Mapping[dsl.Source, parsmod.Source]:
            """Abstract method implementation."""
            return {
                student.join(person, student.surname == person.surname).source: 'pupil',
                student: 'student',
                school: 'school',
            }

    return Dummy


@pytest.fixture(scope='session')
def sink(reference: str) -> type[io.Sink]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Dummy(io.Sink, alias=reference):
        """Dummy sink for unit-testing purposes."""

        class Writer(io.Sink.Writer):
            """Dummy black-hole sink writer."""

            @classmethod
            def write(cls, data: layout.Native, **kwargs: typing.Any) -> None:
                """Do nothing."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

    return Dummy
