"""
ForML testing routine unit tests.
"""
# pylint: disable=protected-access,no-self-use
import abc
import typing

import pytest

from forml.testing import spec, routine as routinemod


class Runner:
    """Runner mock.
    """
    class Outcome:
        """Runner outcome mock
        """
        def __init__(self, init: typing.Optional[spec.Scenario.Exception] = None,
                     apply: typing.Any = None, train: typing.Any = None):
            if init is not None:
                self._raise(init)
            self._apply: typing.Any = apply
            self._train: typing.Any = train

        @staticmethod
        def _raise(exception: spec.Scenario.Exception):
            """Helper for raising exception.
            """
            raise exception.kind(exception.message)

        def train(self):
            """Runner train mode mock.
            """
            if isinstance(self._train, spec.Scenario.Exception):
                self._raise(self._train)
            return self._train

        def apply(self):
            """Runner apply mode mock.
            """
            if isinstance(self._apply, spec.Scenario.Exception):
                self._raise(self._apply)
            return self._apply

    def __init__(self, scenario: spec.Scenario):
        self._scenario: spec.Scenario = scenario

    def __call__(self, _) -> 'Runner.Outcome':
        if self._scenario.outcome is spec.Scenario.Outcome.INIT_RAISES:
            return self.Outcome(init=self._scenario.exception)
        if self._scenario.outcome in {spec.Scenario.Outcome.PLAINAPPLY_RAISES,
                                      spec.Scenario.Outcome.STATEAPPLY_RAISES}:
            return self.Outcome(apply=self._scenario.exception)
        if self._scenario.outcome in {spec.Scenario.Outcome.PLAINAPPLY_RETURNS,
                                      spec.Scenario.Outcome.STATEAPPLY_RETURNS}:
            return self.Outcome(apply=self._scenario.output.apply)
        if self._scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RAISES:
            return self.Outcome(train=self._scenario.exception)
        if self._scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RETURNS:
            return self.Outcome(train=self._scenario.output.train)
        raise RuntimeError('Invalid scenario')


@pytest.fixture(scope='session')
def suite() -> routinemod.Suite:
    """Suite fixture.
    """
    class Suite(routinemod.Suite):
        """Suite mock.
        """
        __operator__ = None

    return Suite()


class Routine(metaclass=abc.ABCMeta):
    """Routine test base class.
    """
    @staticmethod
    @abc.abstractmethod
    def scenario(scenario: spec.Scenario) -> spec.Scenario:
        """Abstract scenario fixture.
        """

    @staticmethod
    @pytest.fixture(scope='session')
    def routine(scenario: spec.Scenario) -> routinemod.Test:
        """Routine fixture.
        """
        return routinemod.Case.select(scenario, Runner(scenario))

    def test_routine(self, routine: routinemod.Test, suite: routinemod.Suite):
        """Routine test case.
        """
        routine(suite)


class TestInitRaises(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(init_raises: spec.Scenario) -> spec.Scenario:
        return init_raises


class TestPlainApplyRaises(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_raises: spec.Scenario) -> spec.Scenario:
        return plainapply_raises


class TestPlainApplyReturns(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_returns: spec.Scenario) -> spec.Scenario:
        return plainapply_returns


class TestStateTrainRaises(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(statetrain_raises: spec.Scenario) -> spec.Scenario:
        return statetrain_raises


class TestStateTrainReturns(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(statetrain_returns: spec.Scenario) -> spec.Scenario:
        return statetrain_returns


class TestStateApplyRaises(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_raises: spec.Scenario) -> spec.Scenario:
        return stateapply_raises


class TestStateApplyReturns(Routine):
    """Routine test.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_returns: spec.Scenario) -> spec.Scenario:
        return stateapply_returns
