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

from forml.flow import _task
from forml.io import feed as feedmod, sink as sinkmod
from forml.io.dsl import function, parser, struct
from forml.io.dsl.struct import frame, kind
from forml.lib.pipeline import topology
from forml.project import distribution, product
from forml.runtime.asset.directory import project as prjmod, lineage as lngmod, generation as genmod


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


class NativeActor(WrappedActor, _task.Actor):
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
def actor(request) -> type[_task.Actor]:
    """Stateful actor fixture."""
    return request.param


@pytest.fixture(scope='session')
def hyperparams() -> typing.Mapping[str, int]:
    """Hyperparams fixture."""
    return dict(a=1, b=2)


@pytest.fixture(scope='session')
def spec(actor: type[_task.Actor], hyperparams):
    """Task spec fixture."""
    return _task.Spec(actor, **hyperparams)


@pytest.fixture(scope='session')
def trainset() -> tuple[str, str]:
    """Trainset fixture."""
    return '123', 'xyz'


@pytest.fixture(scope='session')
def testset(trainset) -> str:
    """Testset fixture."""
    return trainset[0]


@pytest.fixture(scope='session')
def state(spec: _task.Spec, trainset) -> bytes:
    """Actor state fixture."""
    actor = spec()
    actor.train(*trainset)
    return actor.get_state()


@pytest.fixture(scope='session')
def prediction(spec: _task.Spec, state: bytes, testset) -> int:
    """Prediction result fixture."""
    actor = spec()
    actor.set_state(state)
    return actor.apply(testset)


@pytest.fixture(scope='session')
def project_path() -> pathlib.Path:
    """Test project path."""
    return pathlib.Path(__file__).parent / 'helloworld'


@pytest.fixture(scope='session')
def project_package(project_path: pathlib.Path) -> distribution.Package:
    """Test project package fixture."""
    return distribution.Package(project_path)


@pytest.fixture(scope='session')
def project_manifest(project_package: distribution.Package) -> distribution.Manifest:
    """Test project manifest fixture."""
    return project_package.manifest


@pytest.fixture(scope='session')
def project_artifact(project_package: distribution.Package, project_path: str) -> product.Artifact:
    """Test project artifact fixture."""
    return project_package.install(project_path)


@pytest.fixture(scope='session')
def project_name(project_manifest: distribution.Manifest) -> prjmod.Level.Key:
    """Test project name fixture."""
    return project_manifest.name


@pytest.fixture(scope='session')
def project_lineage(project_manifest: distribution.Manifest) -> lngmod.Level.Key:
    """Test project lineage fixture."""
    return project_manifest.version


@pytest.fixture(scope='session')
def valid_generation() -> genmod.Level.Key:
    """Generation fixture."""
    return genmod.Level.Key(1)


@pytest.fixture(scope='function')
def nodes() -> typing.Sequence[uuid.UUID]:
    """Persistent nodes GID fixture."""
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16), uuid.UUID(bytes=b'\x02' * 16)


@pytest.fixture(scope='function')
def states(nodes) -> typing.Mapping[uuid.UUID, bytes]:
    """State IDs to state values mapping fixture."""
    return collections.OrderedDict((n, n.bytes) for n in nodes)


@pytest.fixture(scope='function')
def tag(states: typing.Mapping[uuid.UUID, bytes]) -> genmod.Tag:
    """Tag fixture."""
    return genmod.Tag(
        training=genmod.Tag.Training(datetime.datetime(2019, 4, 1), 123),
        tuning=genmod.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
        states=states.keys(),
    )


@pytest.fixture(scope='session')
def schema() -> frame.Table:
    """Schema fixture."""

    class Human(struct.Schema):
        """Human schema representation."""

        name = struct.Field(kind.String())
        age = struct.Field(kind.Integer())

    return Human


@pytest.fixture(scope='session')
def person() -> frame.Table:
    """Base table fixture."""

    class Person(struct.Schema):
        """Base table."""

        surname = struct.Field(kind.String())
        dob = struct.Field(kind.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: frame.Table) -> frame.Table:
    """Extended table fixture."""

    class Student(person):
        """Extended table."""

        level = struct.Field(kind.Integer())
        score = struct.Field(kind.Float())
        school = struct.Field(kind.Integer())

    return Student


@pytest.fixture(scope='session')
def school() -> frame.Table:
    """School table fixture."""

    class School(struct.Schema):
        """School table."""

        sid = struct.Field(kind.Integer(), 'id')
        name = struct.Field(kind.String())

    return School


@pytest.fixture(scope='session')
def school_ref(school: frame.Table) -> frame.Reference:
    """School table reference fixture."""
    return school.reference('bar')


@pytest.fixture(scope='session')
def query(person: frame.Table, student: frame.Table, school_ref: frame.Reference) -> frame.Query:
    """Query fixture."""
    query = (
        student.join(person, student.surname == person.surname)
        .join(school_ref, student.school == school_ref.sid)
        .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, kind.String()))
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
    reference: str, person: frame.Table, student: frame.Table, school: frame.Table  # pylint: disable=unused-argument
) -> type[feedmod.Provider]:
    """Dummy feed fixture."""

    class Dummy(feedmod.Provider, alias=reference):
        """Dummy feed for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

        @property
        def sources(self) -> typing.Mapping[frame.Source, parser.Source]:
            """Abstract method implementation."""
            return {student.join(person, student.surname == person.surname).source: None, student: None, school: None}

    return Dummy


@pytest.fixture(scope='session')
def sink(reference: str) -> type[sinkmod.Provider]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Dummy(sinkmod.Provider, alias=reference):
        """Dummy sink for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

    return Dummy
