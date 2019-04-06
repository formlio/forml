"""
ForML runtime instruction unit tests.
"""
# pylint: disable=no-self-use
import abc
import pickle
import typing

import pytest

from forml.flow import task
from forml.runtime.assembly import instruction


class Functor(metaclass=abc.ABCMeta):
    """Common functor tests.
    """
    @staticmethod
    @abc.abstractmethod
    def functor(spec: task.Spec) -> instruction.Functional:
        """Functor fixture.
        """

    @staticmethod
    @pytest.fixture(scope='function')
    def shifting() -> typing.Callable[[task.Actor, typing.Any], task.Actor]:
        """Functor fixture.
        """
        class Shifter:
            """Shifting mock.
            """
            def __init__(self):
                self.captured = None
                self.called = False

            def __call__(self, actor: task.Actor, first: typing.Any) -> task.Actor:
                self.captured = first
                return actor

            def __bool__(self):
                return self.called

            def __eq__(self, other):
                return other == self.captured

            def __hash__(self):
                return hash(self.captured)

        return Shifter()

    def test_shiftby(self, functor: instruction.Functor,
                     shifting: typing.Callable[[task.Actor, typing.Any], task.Actor],
                     state: bytes, args: typing.Sequence):
        """Test shiftby.
        """
        functor = functor.shiftby(shifting).shiftby(instruction.Functor.Shifting.state)
        functor(state, None, *args)
        assert not shifting
        functor(state, 'foobar', *args)
        assert shifting == 'foobar'

    def test_serializable(self, functor: instruction.Functor, state: bytes, args: typing.Sequence):
        """Test functor serializability.
        """
        functor = functor.shiftby(instruction.Functor.Shifting.state)
        output = functor(state, *args)
        clone = pickle.loads(pickle.dumps(functor))
        assert isinstance(clone, instruction.Functor)
        assert functor(state, *args) == output


class TestMapper(Functor):
    """Mapper functor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: task.Spec) -> instruction.Mapper:
        """Functor fixture.
        """
        return instruction.Mapper(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(testset) -> typing.Sequence:
        """Functor args fixture.
        """
        return [testset]

    def test_call(self, functor: instruction.Functor, state: bytes, hyperparams, testset, prediction):
        """Test the functor call.
        """
        with pytest.raises(ValueError):
            functor(testset)
        functor = functor.shiftby(instruction.Functor.Shifting.state)
        assert functor(state, testset) == prediction
        functor = functor.shiftby(instruction.Functor.Shifting.params)
        assert functor(hyperparams, state, testset) == prediction


class TestConsumer(Functor):
    """Consumer functor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: task.Spec) -> instruction.Consumer:
        """Functor fixture.
        """
        return instruction.Consumer(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(trainset, testset) -> typing.Sequence:
        """Functor args fixture.
        """
        return [trainset, testset]

    def test_call(self, functor: instruction.Functor, state: bytes, hyperparams, trainset):
        """Test the functor call.
        """
        assert functor(*trainset) == state
        functor = functor.shiftby(instruction.Functor.Shifting.params)
        assert functor(hyperparams, *trainset) == state
