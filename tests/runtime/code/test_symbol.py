"""
ForML runtime symbol unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml import flow
from forml.flow import task
from forml.runtime.code import symbol


class Functor:
    """Common functor tests.
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

    def test_shiftby(self, functor: symbol.Functor, shifting: typing.Callable[[task.Actor, typing.Any], task.Actor],
                     state: bytes, input: typing.Sequence):
        """Test shiftby.
        """
        functor = functor.shiftby(shifting).shiftby(symbol.Functor.Shifting.state)
        functor(state, None, *input)
        assert not shifting
        functor(state, 'foobar', *input)
        assert shifting == 'foobar'


class TestMapper(Functor):
    """Mapper functor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: task.Spec) -> symbol.Mapper:
        """Functor fixture.
        """
        return symbol.Mapper(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def input(testset) -> typing.Sequence:
        """Functor input fixture.
        """
        return [testset]

    def test_call(self, functor: symbol.Functor, state: bytes, hyperparams, testset, prediction):
        """Test the functor call.
        """
        with pytest.raises(ValueError):
            functor(testset)
        functor = functor.shiftby(symbol.Functor.Shifting.state)
        assert functor(state, testset) == prediction
        functor = functor.shiftby(symbol.Functor.Shifting.params)
        assert functor(hyperparams, state, testset) == prediction


class TestConsumer(Functor):
    """Consumer functor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: task.Spec) -> symbol.Consumer:
        """Functor fixture.
        """
        return symbol.Consumer(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def input(trainset, testset) -> typing.Sequence:
        """Functor input fixture.
        """
        return [trainset, testset]

    def test_call(self, functor: symbol.Functor, state: bytes, hyperparams, trainset):
        """Test the functor call.
        """
        assert functor(*trainset) == state
        functor = functor.shiftby(symbol.Functor.Shifting.params)
        assert functor(hyperparams, *trainset) == state
