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

from forml.testing import routine as routinemod
from forml.testing import spec


class Runner:
    """Runner mock."""

    class Outcome:
        """Runner outcome mock"""

        def __init__(
            self,
            init: typing.Optional[spec.Scenario.Exception] = None,
            apply: typing.Any = None,
            train: typing.Any = None,
        ):
            if init is not None:
                self._raise(init)
            self._apply: typing.Any = apply
            self._train: typing.Any = train

        @staticmethod
        def _raise(exception: spec.Scenario.Exception):
            """Helper for raising exception."""
            raise exception.kind(exception.message)

        def train(self):
            """Runner train mode mock."""
            if isinstance(self._train, spec.Scenario.Exception):
                self._raise(self._train)
            return self._train

        def apply(self):
            """Runner apply mode mock."""
            if isinstance(self._apply, spec.Scenario.Exception):
                self._raise(self._apply)
            return self._apply

    def __init__(self, scenario: spec.Scenario):
        self._scenario: spec.Scenario = scenario

    def __call__(self, _) -> 'Runner.Outcome':
        if self._scenario.outcome is spec.Scenario.Outcome.INIT_RAISES:
            return self.Outcome(init=self._scenario.exception)
        if self._scenario.outcome in {spec.Scenario.Outcome.PLAINAPPLY_RAISES, spec.Scenario.Outcome.STATEAPPLY_RAISES}:
            return self.Outcome(apply=self._scenario.exception)
        if self._scenario.outcome in {
            spec.Scenario.Outcome.PLAINAPPLY_RETURNS,
            spec.Scenario.Outcome.STATEAPPLY_RETURNS,
        }:
            return self.Outcome(apply=self._scenario.output.apply)
        if self._scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RAISES:
            return self.Outcome(train=self._scenario.exception)
        if self._scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RETURNS:
            return self.Outcome(train=self._scenario.output.train)
        raise RuntimeError('Invalid scenario')


@pytest.fixture(scope='session')
def suite() -> routinemod.Suite:
    """Suite fixture."""

    class Suite(routinemod.Suite):
        """Suite mock."""

        __operator__ = None

    return Suite()


class Routine(metaclass=abc.ABCMeta):
    """Routine test base class."""

    @staticmethod
    @abc.abstractmethod
    def scenario(scenario: spec.Scenario) -> spec.Scenario:
        """Abstract scenario fixture."""

    @staticmethod
    @pytest.fixture(scope='session')
    def routine(scenario: spec.Scenario) -> routinemod.Test:
        """Routine fixture."""
        return routinemod.Case.select(scenario, Runner(scenario))

    def test_routine(self, routine: routinemod.Test, suite: routinemod.Suite):
        """Routine test case."""
        routine(suite)


class TestInitRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(init_raises: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return init_raises


class TestPlainApplyRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_raises: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return plainapply_raises


class TestPlainApplyReturns(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(plainapply_returns: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return plainapply_returns


class TestStateTrainRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(statetrain_raises: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return statetrain_raises


class TestStateTrainReturns(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(statetrain_returns: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return statetrain_returns


class TestStateApplyRaises(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_raises: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return stateapply_raises


class TestStateApplyReturns(Routine):
    """Routine test."""

    @staticmethod
    @pytest.fixture(scope='session')
    def scenario(stateapply_returns: spec.Scenario) -> spec.Scenario:  # pylint: disable=arguments-renamed
        return stateapply_returns
