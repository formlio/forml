"""
Flow task unit tests.
"""
# pylint: disable=no-self-use
import pickle
import typing

import pytest

from forml.flow import task


class TestActor:
    """Actor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def instance(spec: task.Spec) -> task.Actor:
        """Instance fixture.
        """
        return spec()

    def test_train(self, instance: task.Actor, trainset, testset, prediction):
        """Test actor training.
        """
        assert instance.is_stateful()
        instance.train(*trainset)
        assert instance.apply(testset) == prediction

    def test_params(self, instance: task.Actor, hyperparams):
        """Params setter/getter tests.
        """
        orig = instance.get_params()
        assert orig == hyperparams
        assert 'x' not in orig
        instance.set_params(x=100)
        assert instance.get_params()['x'] == 100

    def test_state(self, instance: task.Actor, trainset, state, testset, prediction):
        """Testing actor statefulness.
        """
        instance.train(*trainset)
        assert instance.predict(testset) == prediction
        assert instance.get_state() == state
        instance.train('foo', 'bar')  # retraining to change the state
        assert instance.predict(testset) != prediction
        assert 'x' not in instance.get_params()
        instance.set_params(x=100)
        instance.set_state(state)
        assert instance.get_params()['x'] == 100  # state shouldn't override parameter setting

    def test_spec(self, actor: typing.Type[task.Actor], hyperparams: typing.Mapping[str, int], spec: task.Spec):
        """Test the spec creation of the actor class.
        """
        assert actor.spec(**hyperparams) == spec

    def test_serializable(self, instance: task.Actor, trainset, testset, prediction):
        """Test actor serializability.
        """
        instance.train(*trainset)
        assert pickle.loads(pickle.dumps(instance)).predict(testset) == prediction


class TestSpec:
    """Task spec unit tests.
    """
    def test_hashable(self, spec: task.Spec):
        """Test spec hashability.
        """
        assert spec in {spec}

    def test_serializable(self, spec: task.Spec, actor: typing.Type[task.Actor]):
        """Test spec serializability.
        """
        assert pickle.loads(pickle.dumps(spec)).actor == actor

    def test_instantiate(self, spec: task.Spec):
        """Testing specto actor instantiation.
        """
        assert spec(b=3).get_params() == {**spec.kwargs, 'b': 3}
