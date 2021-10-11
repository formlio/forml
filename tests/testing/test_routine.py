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
ForML testing routine unit tests.
"""
# pylint: disable=protected-access,no-self-use
import abc
import typing

import pytest

from forml import testing
from forml.testing import _routine as routinemod


class Runner:
    """Runner mock."""

    class Outcome:
        """Runner outcome mock"""

        def __init__(
            self,
            init: typing.Optional[testing.Scenario.Exception] = None,
            apply: typing.Any = None,
            train: typing.Any = None,
        ):
            if init is not None:
                self._raise(init)
            self._apply: typing.Any = apply
            self._train: typing.Any = train

        @staticmethod
        def _raise(exception: testing.Scenario.Exception):
            """Helper for raising exception."""
            raise exception.kind(exception.message)

        def train(self):
            """Runner train mode mock."""
            if isinstance(self._train, testing.Scenario.Exception):
                self._raise(self._train)
            return self._train

        def apply(self):
            """Runner apply mode mock."""
            if isinstance(self._apply, testing.Scenario.Exception):
                self._raise(self._apply)
            return self._apply

    def __init__(self, scenario: testing.Scenario):
        self._scenario: testing.Scenario = scenario

    def __call__(self, _) -> 'Runner.Outcome':
        if self._scenario.outcome is testing.Scenario.Outcome.INIT_RAISES:
            return self.Outcome(init=self._scenario.exception)
        if self._scenario.outcome in {
            testing.Scenario.Outcome.PLAINAPPLY_RAISES,
            testing.Scenario.Outcome.STATEAPPLY_RAISES,
        }:
            return self.Outcome(apply=self._scenario.exception)
        if self._scenario.outcome in {
            testing.Scenario.Outcome.PLAINAPPLY_RETURNS,
            testing.Scenario.Outcome.STATEAPPLY_RETURNS,
        }:
            return self.Outcome(apply=self._scenario.output.apply)
        if self._scenario.outcome is testing.Scenario.Outcome.STATETRAIN_RAISES:
            return self.Outcome(train=self._scenario.exception)
        raise RuntimeError('Invalid scenario')


@pytest.fixture(scope='session')
def suite() -> testing.Suite:
    """Suite fixture."""

    class Suite(testing.Suite):
        """Suite mock."""

        __operator__ = None

    return Suite()


class Routine(metaclass=abc.ABCMeta):
    """Routine test base class."""

    @staticmethod
    @abc.abstractmethod
    def scenario(scenario: testing.Scenario) -> testing.Scenario:
        """Abstract scenario fixture."""

    @staticmethod
    @pytest.fixture(scope='session')
    def routine(scenario: testing.Scenario) -> routinemod.Test:
        """Routine fixture."""
        return routinemod.Case.select(scenario, Runner(scenario))

    def test_routine(self, routine: routinemod.Test, suite: testing.Suite):
        """Routine test case."""
        routine(suite)


class TestInitRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(init_raises: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return init_raises


class TestPlainApplyRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_raises: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return plainapply_raises


class TestPlainApplyReturns(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_returns: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return plainapply_returns


class TestStateTrainRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(statetrain_raises: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return statetrain_raises


class TestStateApplyRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_raises: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return stateapply_raises


class TestStateApplyReturns(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_returns: testing.Scenario) -> testing.Scenario:  # pylint: disable=arguments-renamed
        return stateapply_returns
