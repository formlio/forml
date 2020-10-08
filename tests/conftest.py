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

from forml.flow import task
from forml.lib.flow.actor import wrapped
from forml.project import distribution, product
from forml.runtime.asset.directory import project as prjmod, lineage as lngmod, generation as genmod


class WrappedActor:
    """Actor to-be mockup.
    """
    def __init__(self, **params):
        self._model = list()
        self._params = params

    def train(self, features, labels) -> None:
        """Train to-be handler.
        """
        self._model.append((features, labels))

    def predict(self, features) -> int:
        """Apply to-be handler.
        """
        if not self._model:
            raise ValueError('Not Fitted')
        return hash(features) ^ hash(tuple(self._model)) ^ hash(tuple(sorted(self._params.items())))

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        """Get hyper-parameters of this actor.
        """
        return self._params

    def set_params(self, **params: typing.Any):
        """Set hyper-parameters of this actor.
        """
        self._params.update(params)


class NativeActor(WrappedActor, task.Actor):
    """Actor implementation.
    """
    def apply(self, *features: typing.Any) -> typing.Any:
        """Native apply method.
        """
        return self.predict(features[0])


@pytest.fixture(scope='session', params=(NativeActor, wrapped.Class.actor(WrappedActor, apply='predict')))
def actor(request) -> typing.Type[task.Actor]:
    """Stateful actor fixture.
    """
    return request.param


@pytest.fixture(scope='session')
def hyperparams() -> typing.Mapping[str, int]:
    """Hyperparams fixture.
    """
    return dict(a=1, b=2)


@pytest.fixture(scope='session')
def spec(actor: typing.Type[task.Actor], hyperparams):
    """Task spec fixture.
    """
    return task.Spec(actor, **hyperparams)


@pytest.fixture(scope='session')
def trainset() -> typing.Tuple[str, str]:
    """Trainset fixture.
    """
    return '123', 'xyz'


@pytest.fixture(scope='session')
def testset(trainset) -> str:
    """Testset fixture.
    """
    return trainset[0]


@pytest.fixture(scope='session')
def state(spec: task.Spec, trainset) -> bytes:
    """Actor state fixture.
    """
    actor = spec()
    actor.train(*trainset)
    return actor.get_state()


@pytest.fixture(scope='session')
def prediction(spec: task.Spec, state: bytes, testset) -> int:
    """Prediction result fixture.
    """
    actor = spec()
    actor.set_state(state)
    return actor.apply(testset)


@pytest.fixture(scope='session')
def project_path() -> pathlib.Path:
    """Test project path.
    """
    return pathlib.Path(__file__).parent / 'helloworld'


@pytest.fixture(scope='session')
def project_package(project_path: pathlib.Path) -> distribution.Package:
    """Test project package fixture.
    """
    return distribution.Package(project_path)


@pytest.fixture(scope='session')
def project_manifest(project_package: distribution.Package) -> distribution.Manifest:
    """Test project manifest fixture.
    """
    return project_package.manifest


@pytest.fixture(scope='session')
def project_artifact(project_package: distribution.Package, project_path: str) -> product.Artifact:
    """Test project artifact fixture.
    """
    return project_package.install(project_path)


@pytest.fixture(scope='session')
def project_name(project_package: distribution.Package) -> prjmod.Level.Key:
    """Test project name fixture.
    """
    return project_package.manifest.name


@pytest.fixture(scope='session')
def project_lineage(project_package: distribution.Package) -> lngmod.Level.Key:
    """Test project lineage fixture.
    """
    return project_package.manifest.version


@pytest.fixture(scope='session')
def valid_generation() -> genmod.Level.Key:
    """Generation fixture.
    """
    return genmod.Level.Key(1)


@pytest.fixture(scope='function')
def nodes() -> typing.Sequence[uuid.UUID]:
    """Persistent nodes GID fixture.
    """
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16), uuid.UUID(bytes=b'\x02' * 16)


@pytest.fixture(scope='function')
def states(nodes) -> typing.Mapping[uuid.UUID, bytes]:
    """State IDs to state values mapping fixture.
    """
    return collections.OrderedDict((n, n.bytes) for n in nodes)


@pytest.fixture(scope='function')
def tag(states: typing.Mapping[uuid.UUID, bytes]) -> genmod.Tag:
    """Tag fixture.
    """
    return genmod.Tag(training=genmod.Tag.Training(datetime.datetime(2019, 4, 1), 123),
                      tuning=genmod.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
                      states=states.keys())
