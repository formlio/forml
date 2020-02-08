"""
ForML testing unit tests fixtures.
"""
# pylint: disable=no-self-use


import typing

import pytest

from forml.testing import spec


@pytest.fixture(scope='session')
def exception() -> spec.Scenario.Exception:
    """Exception fixture.

    Returns: Exception type.
    """
    return spec.Scenario.Exception(RuntimeError, 'This is an Error')


@pytest.fixture(scope='session')
def apply_input() -> str:
    """IO fixture.

    Returns: String value.
    """
    return 'foo'


@pytest.fixture(scope='session')
def apply_output() -> str:
    """IO fixture.

    Returns: String value.
    """
    return 'bar'


@pytest.fixture(scope='session')
def train_input() -> str:
    """IO fixture.

    Returns: String value.
    """
    return 'foo'


@pytest.fixture(scope='session')
def label_input() -> str:
    """IO fixture.

    Returns: String value.
    """
    return 'bar'


@pytest.fixture(scope='session')
def train_output() -> str:
    """IO fixture.

    Returns: String value.
    """
    return 'baz'


@pytest.fixture(scope='session')
def init_raises(hyperparams: typing.Mapping[str, int], exception: spec.Scenario.Exception) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def plainapply_raises(hyperparams: typing.Mapping[str, int], apply_input: str,
                      exception: spec.Scenario.Exception) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).apply(apply_input).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def plainapply_returns(hyperparams: typing.Mapping[str, int], apply_input: str, apply_output: str) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).apply(apply_input).returns(apply_output)


@pytest.fixture(scope='session')
def statetrain_raises(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                      exception: spec.Scenario.Exception) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).train(train_input, label_input).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def statetrain_returns(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                       train_output: str) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).train(train_input, label_input).returns(train_output)


@pytest.fixture(scope='session')
def stateapply_raises(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                      apply_input, exception: spec.Scenario.Exception) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).train(
        train_input, label_input).apply(apply_input).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def stateapply_returns(hyperparams: typing.Mapping[str, int], train_input: str, label_input: str,
                       apply_input: str, apply_output: str) -> spec.Scenario:
    """Scenario fixture.
    """
    return spec.Case(**hyperparams).train(train_input, label_input).apply(apply_input).returns(apply_output)
