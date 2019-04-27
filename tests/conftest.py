"""
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.stdlib import actor as actlib
from forml.flow import task


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


@pytest.fixture(scope='session', params=(NativeActor, actlib.Wrapped.actor(WrappedActor, apply='predict')))
def actor(request) -> typing.Type[task.Actor]:
    """Actor fixture.
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
