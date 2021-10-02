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

from forml import flow, io, project
from forml.io import dsl
from forml.io.dsl import function, parser
from forml.lib.pipeline import topology
from forml.runtime import asset


class WrappedActor:
    """Actor to-be mockup."""

    def __init__(self, **params):
        self._model = []
        self._params = params

    def train(self, features, labels) -> None:
        """Train to-be handler."""
        self._model.append((features, labels))

    def predict(self, features) -> int:
        """Apply to-be handler."""
        if not self._model:
            raise ValueError('Not Fitted')
        return hash(features) ^ hash(tuple(self._model)) ^ hash(tuple(sorted(self._params.items())))

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
def trainset() -> tuple[str, str]:
    """Trainset fixture."""
    return '123', 'xyz'


@pytest.fixture(scope='session')
def testset(trainset) -> str:
    """Testset fixture."""
    return trainset[0]


@pytest.fixture(scope='session')
def state(spec: flow.Spec, trainset) -> bytes:
    """Actor state fixture."""
    actor = spec()
    actor.train(*trainset)
    return actor.get_state()


@pytest.fixture(scope='session')
def prediction(spec: flow.Spec, state: bytes, testset) -> int:
    """Prediction result fixture."""
    actor = spec()
    actor.set_state(state)
    return actor.apply(testset)


@pytest.fixture(scope='session')
def project_path() -> pathlib.Path:
    """Test project path."""
    return pathlib.Path(__file__).parent / 'helloworld'


@pytest.fixture(scope='session')
def project_package(project_path: pathlib.Path) -> project.Package:
    """Test project package fixture."""
    return project.Package(project_path)


@pytest.fixture(scope='session')
def project_manifest(project_package: project.Package) -> project.Manifest:
    """Test project manifest fixture."""
    return project_package.manifest


@pytest.fixture(scope='session')
def project_artifact(project_package: project.Package, project_path: str) -> project.Artifact:
    """Test project artifact fixture."""
    return project_package.install(project_path)


@pytest.fixture(scope='session')
def project_name(project_manifest: project.Manifest) -> asset.Project.Key:
    """Test project name fixture."""
    return project_manifest.name


@pytest.fixture(scope='session')
def project_lineage(project_manifest: project.Manifest) -> asset.Lineage.Key:
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
def schema() -> dsl.Table:
    """Schema fixture."""

    class Human(dsl.Schema):
        """Human schema representation."""

        name = dsl.Field(dsl.String())
        age = dsl.Field(dsl.Integer())

    return Human


@pytest.fixture(scope='session')
def person() -> dsl.Table:
    """Base table fixture."""

    class Person(dsl.Schema):
        """Base table."""

        surname = dsl.Field(dsl.String())
        dob = dsl.Field(dsl.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: dsl.Table) -> dsl.Table:
    """Extended table fixture."""

    class Student(person):
        """Extended table."""

        level = dsl.Field(dsl.Integer())
        score = dsl.Field(dsl.Float())
        school = dsl.Field(dsl.Integer())

    return Student


@pytest.fixture(scope='session')
def school() -> dsl.Table:
    """School table fixture."""

    class School(dsl.Schema):
        """School table."""

        sid = dsl.Field(dsl.Integer(), 'id')
        name = dsl.Field(dsl.String())

    return School


@pytest.fixture(scope='session')
def school_ref(school: dsl.Table) -> dsl.Reference:
    """School table reference fixture."""
    return school.reference('bar')


@pytest.fixture(scope='session')
def query(person: dsl.Table, student: dsl.Table, school_ref: dsl.Reference) -> dsl.Query:
    """Query fixture."""
    query = (
        student.join(person, student.surname == person.surname)
        .join(school_ref, student.school == school_ref.sid)
        .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, dsl.String()))
        .where(student.score < 2)
        .orderby(student.level, student.score)
        .limit(10)
    )
    return query


@pytest.fixture(scope='session')
def reference() -> str:
    """Dummy feed/sink reference fixture"""
    return 'dummy'


@pytest.fixture(scope='session')
def feed(
    reference: str, person: dsl.Table, student: dsl.Table, school: dsl.Table  # pylint: disable=unused-argument
) -> type[io.Feed]:
    """Dummy feed fixture."""

    class Dummy(io.Feed, alias=reference):
        """Dummy feed for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

        @property
        def sources(self) -> typing.Mapping[dsl.Source, parser.Source]:
            """Abstract method implementation."""
            return {student.join(person, student.surname == person.surname).source: None, student: None, school: None}

    return Dummy


@pytest.fixture(scope='session')
def sink(reference: str) -> type[io.Sink]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Dummy(io.Sink, alias=reference):
        """Dummy sink for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

    return Dummy
