"""
ForML testing unit tests.
"""
# pylint: disable=protected-access,no-self-use

import typing

import pytest

from forml.testing import spec


class TestCase:
    """Testing Case unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def exception() -> typing.Type[Exception]:
        """Exception fixture.

        Returns: Exception type.
        """
        return AssertionError

    @staticmethod
    @pytest.fixture(scope='session')
    def apply_input() -> str:
        """IO fixture.

        Returns: String value.
        """
        return 'foo'

    @staticmethod
    @pytest.fixture(scope='session')
    def apply_output() -> str:
        """IO fixture.

        Returns: String value.
        """
        return 'bar'

    @staticmethod
    @pytest.fixture(scope='session')
    def train_input() -> str:
        """IO fixture.

        Returns: String value.
        """
        return 'foo'

    @staticmethod
    @pytest.fixture(scope='session')
    def label_input() -> str:
        """IO fixture.

        Returns: String value.
        """
        return 'bar'

    @staticmethod
    @pytest.fixture(scope='session')
    def train_output() -> str:
        """IO fixture.

        Returns: String value.
        """
        return 'baz'

    @staticmethod
    @pytest.fixture(scope='session')
    def init_raises(hyperparams: typing.Mapping[str, int], exception: typing.Type[Exception]) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).raises(exception)

    @staticmethod
    @pytest.fixture(scope='session')
    def plainapply_raises(hyperparams: typing.Mapping[str, int], apply_input: str,
                          exception: typing.Type[Exception]) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).apply(apply_input).raises(exception)

    @staticmethod
    @pytest.fixture(scope='session')
    def plainapply_returns(hyperparams: typing.Mapping[str, int], apply_input: str, apply_output: str) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).apply(apply_input).returns(apply_output)

    @staticmethod
    @pytest.fixture(scope='session')
    def statetrain_raises(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                          exception: typing.Type[Exception]) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).train(train_input, label_input).raises(exception)

    @staticmethod
    @pytest.fixture(scope='session')
    def statetrain_returns(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                           train_output: str) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).train(train_input, label_input).returns(train_output)

    @staticmethod
    @pytest.fixture(scope='session')
    def stateapply_raises(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                          apply_input, exception: typing.Type[Exception]) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).train(train_input, label_input).apply(apply_input).raises(exception)

    @staticmethod
    @pytest.fixture(scope='session')
    def stateapply_returns(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                           apply_input: str, apply_output: str) -> spec.Scenario:
        """Scenario fixture.
        """
        return spec.Case(**hyperparams).train(train_input, label_input).apply(apply_input).returns(apply_output)

    def test_init_raises(self, init_raises: spec.Scenario, hyperparams: typing.Mapping[str, int], exception: Exception):
        """Scenario spec test case.
        """
        assert init_raises.outcome == spec.Scenario.Outcome.INIT_RAISES
        assert init_raises.params.kwargs == hyperparams
        assert init_raises.exception == exception

    def test_plainapply_raises(self, plainapply_raises: spec.Scenario, hyperparams: typing.Mapping[str, int],
                               apply_input: str, exception: Exception):
        """Scenario spec test case.
        """
        assert plainapply_raises.outcome == spec.Scenario.Outcome.PLAINAPPLY_RAISES
        assert plainapply_raises.params.kwargs == hyperparams
        assert plainapply_raises.input.apply == apply_input
        assert plainapply_raises.exception == exception

    def test_plainapply_returns(self, plainapply_returns: spec.Scenario, hyperparams: typing.Mapping[str, int],
                                apply_input: str, apply_output: str):
        """Scenario spec test case.
        """
        assert plainapply_returns.outcome == spec.Scenario.Outcome.PLAINAPPLY_RETURNS
        assert plainapply_returns.params.kwargs == hyperparams
        assert plainapply_returns.input.apply == apply_input
        assert plainapply_returns.output.apply == apply_output

    def test_statetrain_raises(self, statetrain_raises: spec.Scenario, hyperparams: typing.Mapping[str, int],
                               train_input: str, label_input: str, exception: Exception):
        """Scenario spec test case.
        """
        assert statetrain_raises.outcome == spec.Scenario.Outcome.STATETRAIN_RAISES
        assert statetrain_raises.params.kwargs == hyperparams
        assert statetrain_raises.input.train == train_input
        assert statetrain_raises.input.label == label_input
        assert statetrain_raises.exception == exception

    def test_statetrain_returns(self, statetrain_returns: spec.Scenario, hyperparams: typing.Mapping[str, int],
                                train_input: str, label_input: str, train_output: str):
        """Scenario spec test case.
        """
        assert statetrain_returns.outcome == spec.Scenario.Outcome.STATETRAIN_RETURNS
        assert statetrain_returns.params.kwargs == hyperparams
        assert statetrain_returns.input.train == train_input
        assert statetrain_returns.input.label == label_input
        assert statetrain_returns.output.train == train_output

    def test_stateapply_raises(self, stateapply_raises: spec.Scenario, hyperparams: typing.Mapping[str, int],
                               train_input: str, label_input: str, apply_input: str, exception: Exception):
        """Scenario spec test case.
        """
        assert stateapply_raises.outcome == spec.Scenario.Outcome.STATEAPPLY_RAISES
        assert stateapply_raises.params.kwargs == hyperparams
        assert stateapply_raises.input.train == train_input
        assert stateapply_raises.input.label == label_input
        assert stateapply_raises.input.apply == apply_input
        assert stateapply_raises.exception == exception

    def test_stateapply_returns(self, stateapply_returns: spec.Scenario, hyperparams: typing.Mapping[str, int],
                                train_input: str, label_input: str, apply_input: str, apply_output: str):
        """Scenario spec test case.
        """
        assert stateapply_returns.outcome == spec.Scenario.Outcome.STATEAPPLY_RETURNS
        assert stateapply_returns.params.kwargs == hyperparams
        assert stateapply_returns.input.train == train_input
        assert stateapply_returns.input.label == label_input
        assert stateapply_returns.input.apply == apply_input
        assert stateapply_returns.output.apply == apply_output
