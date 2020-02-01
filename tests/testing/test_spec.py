"""
ForML testing unit tests.
"""
# pylint: disable=protected-access,no-self-use

import pytest
import typing

from forml.testing import spec


class TestCase:
    """Testing Case unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def case(hyperparams: typing.Mapping[str, int]) -> spec.Case:
        """Case fixture.
        """
        return spec.Case(**hyperparams)

    @staticmethod
    @pytest.fixture(scope='session')
    def params(hyperparams: typing.Mapping[str, int]) -> spec.Scenario.Params:
        """Params fixture.
        """
        return spec.Scenario.Params(**hyperparams)

    def test_raises(self, case: spec.Case, params: spec.Scenario.Params):
        """Raises assertion test.
        """
        exception = RuntimeError
        assert case.raises(exception) == spec.Scenario(params, exception=exception)
        assert case.apply('foo').raises(exception) == spec.Scenario(
            params, spec.Scenario.Data(apply='foo'), exception=exception)
        assert case.train('foo', 'bar').raises(exception) == spec.Scenario(
            params, spec.Scenario.Data(train='foo', label='bar'), exception=exception)
        assert case.train('foo', 'bar').apply('baz').raises(exception) == spec.Scenario(
            params, spec.Scenario.Data(apply='baz', train='foo', label='bar'), exception=exception)

    def test_returns(self, case: spec.Case, params: spec.Scenario.Params):
        """Returns assertion test.
        """
        assert case.apply('foo').returns('FOO') == spec.Scenario(
            params, spec.Scenario.Data(apply='foo'), spec.Scenario.Data(apply='FOO'))
        assert case.train('foo', 'bar').returns('FOO', 'BAR') == spec.Scenario(
            params, spec.Scenario.Data(train='foo', label='bar'), spec.Scenario.Data(train='FOO', label='BAR'))
        assert case.train('foo', 'bar').apply('baz').returns('BAZ') == spec.Scenario(
            params, spec.Scenario.Data(apply='baz', train='foo', label='bar'), spec.Scenario.Data(apply='BAZ'))
