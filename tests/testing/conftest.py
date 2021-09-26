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
ForML testing unit tests fixtures.
"""
# pylint: disable=no-self-use


import typing

import pytest

from forml import testing
from forml.testing import spec


@pytest.fixture(scope='session')
def exception() -> spec.Scenario.Exception:
    """Exception fixture.

    Returns:
        Exception type.
    """
    return spec.Scenario.Exception(RuntimeError, 'This is an Error')


@pytest.fixture(scope='session')
def apply_input() -> str:
    """IO fixture.

    Returns:
        String value.
    """
    return 'foo'


@pytest.fixture(scope='session')
def apply_output() -> str:
    """IO fixture.

    Returns:
        String value.
    """
    return 'bar'


@pytest.fixture(scope='session')
def train_input() -> str:
    """IO fixture.

    Returns:
        String value.
    """
    return 'foo'


@pytest.fixture(scope='session')
def label_input() -> str:
    """IO fixture.

    Returns:
        String value.
    """
    return 'bar'


@pytest.fixture(scope='session')
def train_output() -> str:
    """IO fixture.

    Returns:
        String value.
    """
    return 'baz'


@pytest.fixture(scope='session')
def init_raises(hyperparams: typing.Mapping[str, int], exception: spec.Scenario.Exception) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def plainapply_raises(
    hyperparams: typing.Mapping[str, int], apply_input: str, exception: spec.Scenario.Exception
) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).apply(apply_input).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def plainapply_returns(hyperparams: typing.Mapping[str, int], apply_input: str, apply_output: str) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).apply(apply_input).returns(apply_output)


@pytest.fixture(scope='session')
def statetrain_raises(
    hyperparams: typing.Mapping[str, int], train_input: str, label_input: str, exception: spec.Scenario.Exception
) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).train(train_input, label_input).raises(exception.kind, exception.message)


@pytest.fixture(scope='session')
def statetrain_returns(
    hyperparams: typing.Mapping[str, int], train_input: str, label_input: str, train_output: str
) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).train(train_input, label_input).returns(train_output)


@pytest.fixture(scope='session')
def stateapply_raises(
    hyperparams: typing.Mapping[str, int],
    train_input: str,
    label_input: str,
    apply_input,
    exception: spec.Scenario.Exception,
) -> spec.Scenario:
    """Scenario fixture."""
    return (
        testing.Case(**hyperparams)
        .train(train_input, label_input)
        .apply(apply_input)
        .raises(exception.kind, exception.message)
    )


@pytest.fixture(scope='session')
def stateapply_returns(
    hyperparams: typing.Mapping[str, int], train_input: str, label_input: str, apply_input: str, apply_output: str
) -> spec.Scenario:
    """Scenario fixture."""
    return testing.Case(**hyperparams).train(train_input, label_input).apply(apply_input).returns(apply_output)
