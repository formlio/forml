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
Testing routines.
"""
import abc
import contextlib
import io
import logging
import string
import typing
import unittest

from forml import flow
from forml.conf.parsed import provider as provcfg
from forml.testing import _facility, _spec

if typing.TYPE_CHECKING:
    from forml import testing

LOGGER = logging.getLogger(__name__)


class Suite(unittest.TestCase, metaclass=abc.ABCMeta):
    """Abstract base class of operator testing suite."""

    def __repr__(self):
        return self.__class__.__name__

    @property
    @abc.abstractmethod
    def __operator__(self) -> type[flow.Operator]:
        """Operator instance."""


class Meta(abc.ABCMeta):
    """Meta class for generating unittest classes out of our framework."""

    def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any], **kwargs):
        if not any(issubclass(b, Suite) for b in bases):
            raise TypeError(f'{name} not a valid {Suite.__name__}')
        for title, scenario in [(t, s) for t, s in namespace.items() if isinstance(s, _spec.Scenario)]:
            namespace[f'test_{title}'] = Case(title, scenario)
            del namespace[title]
        return super().__new__(mcs, name, bases, namespace)


class Test:
    """Base class for test implementations."""

    def __init__(self, launcher: _facility.Launcher):
        self._launcher: _facility.Launcher = launcher

    def __call__(self, suite: 'testing.Suite') -> None:
        action: typing.Optional[_facility.Launcher.Action] = self.init(suite)
        if action:
            with contextlib.redirect_stderr(io.StringIO()) as stderr, self.raises(suite):
                self.matches(suite, self.test(action))
            LOGGER.debug('Captured output: %s', stderr.getvalue())

    def init(self, suite: 'testing.Suite') -> _facility.Launcher.Action:
        """Test init phase.

        Args:
            suite: Testing suite.

        Returns:
            Runner launcher instance.
        """
        return self._launcher(suite.__operator__)

    def raises(self, suite: 'testing.Suite') -> typing.ContextManager:  # pylint: disable=unused-argument, no-self-use
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.

        Returns:
            an assertRaises context manager.
        """
        return contextlib.nullcontext()

    def matches(self, suite: 'testing.Suite', value: typing.Any) -> None:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.
            value: Tested value..
        """

    def test(self, action: _facility.Launcher.Action) -> typing.Any:
        """Test subject logic.

        Args:
            action: Launcher action instance.

        Returns:
            Produced value.
        """


class RaisableTest(Test):
    """Base test class for raising test cases."""

    def __init__(self, launcher: _facility.Launcher, exception: 'testing.Scenario.Exception'):
        super().__init__(launcher)
        self._exception: _spec.Scenario.Exception = exception

    def raises(self, suite: 'testing.Suite') -> typing.ContextManager:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.

        Returns:
            an assertRaises context manager.
        """
        if self._exception.message is not None:
            return suite.assertRaisesRegex(self._exception.kind, self._exception.message)
        return suite.assertRaises(self._exception.kind)


class ReturnableTest(Test):
    """Base test class for returning test cases."""

    def __init__(self, launcher: _facility.Launcher, output: 'testing.Scenario.Output'):
        super().__init__(launcher)
        self._output: _spec.Scenario.Output = output

    def matches(self, suite: 'testing.Suite', value: typing.Any) -> None:
        """Result matching entrypoint.

        Args:
            suite: Tested suite.
            value: Tested value.
        """
        self._assert(suite, self._output.value, value, self._output.matcher)

    @classmethod
    def _assert(
        cls,
        suite: 'testing.Suite',
        expected: typing.Any,
        actual: typing.Any,
        matcher: typing.Optional[_spec.Matcher] = None,
    ) -> None:
        """Match assertion.

        Args:
            suite: Tested suite.
            expected: Expected value.
            actual: Actual value.
            matcher: Optional matcher function.
        """
        msg = f'\nExpected: {expected}\nActual: {actual}'
        if matcher is not None:
            suite.assertTrue(matcher(expected, actual), msg)
        else:
            suite.assertEqual(expected, actual, msg)


class TestInitRaises(RaisableTest, Test):
    """Test composite."""

    def init(self, suite: 'testing.Suite') -> _facility.Launcher.Action:
        with self.raises(suite):
            return super().init(suite)


class PlainApplyTest(Test):
    """Testcase logic."""

    def test(self, action: _facility.Launcher.Action) -> typing.Any:
        return action.apply()


class TestPlainApplyReturns(ReturnableTest, PlainApplyTest):
    """Test composite."""


class TestPlainApplyRaises(RaisableTest, PlainApplyTest):
    """Test composite."""


class StateTrainTest(Test):
    """Testcase logic."""

    def test(self, action: _facility.Launcher.Action) -> typing.Any:
        return action.train_return()


class TestStateTrainReturns(ReturnableTest, StateTrainTest):
    """Test composite."""

    @classmethod
    def _assert(
        cls,
        suite: 'testing.Suite',
        expected: tuple[flow.Features, flow.Labels],
        actual: tuple[flow.Features, flow.Labels],
        matcher: typing.Optional[typing.Union[_spec.Matcher, tuple[_spec.Matcher, _spec.Matcher]]] = None,
    ) -> None:
        if not isinstance(matcher, typing.Sequence):
            matcher = matcher, matcher
        super()._assert(suite, expected[0], actual[0], matcher[0])
        if expected[1] is not None:
            super()._assert(suite, expected[1], actual[1], matcher[1])


class TestStateTrainRaises(RaisableTest, StateTrainTest):
    """Test composite."""


class StateApplyTest(PlainApplyTest):
    """Testcase logic."""

    def init(self, suite: 'testing.Suite') -> _facility.Launcher.Action:
        launcher = super().init(suite)
        launcher.train_call()
        return launcher


class TestStateApplyReturns(ReturnableTest, StateApplyTest):
    """Test composite."""


class TestStateApplyRaises(RaisableTest, StateApplyTest):
    """Test composite."""


class Case:
    """Test case routine."""

    def __init__(self, name: str, scenario: 'testing.Scenario', launcher: provcfg.Runner = provcfg.Runner.default):
        self._name: str = name
        launcher = _facility.Launcher(scenario.params, scenario.input, launcher)
        self._test: Test = self.select(scenario, launcher)

    @staticmethod
    def select(scenario: 'testing.Scenario', launcher: _facility.Launcher) -> Test:
        """Selecting and setting up the test implementation for given scenario.

        Args:
            scenario: Testing scenario specification.
            launcher: Test launcher with given operator.

        Returns:
            Test case instance.
        """
        if scenario.outcome is _spec.Scenario.Outcome.INIT_RAISES:
            return TestInitRaises(launcher, scenario.exception)
        if scenario.outcome is _spec.Scenario.Outcome.PLAINAPPLY_RAISES:
            return TestPlainApplyRaises(launcher, scenario.exception)
        if scenario.outcome is _spec.Scenario.Outcome.STATETRAIN_RAISES:
            return TestStateTrainRaises(launcher, scenario.exception)
        if scenario.outcome is _spec.Scenario.Outcome.STATEAPPLY_RAISES:
            return TestStateApplyRaises(launcher, scenario.exception)
        if scenario.outcome is _spec.Scenario.Outcome.PLAINAPPLY_RETURNS:
            return TestPlainApplyReturns(launcher, scenario.output)
        if scenario.outcome is _spec.Scenario.Outcome.STATETRAIN_RETURNS:
            return TestStateTrainReturns(launcher, scenario.output)
        if scenario.outcome is _spec.Scenario.Outcome.STATEAPPLY_RETURNS:
            return TestStateApplyReturns(launcher, scenario.output)
        raise RuntimeError('Unexpected scenario outcome')

    def __get__(self, suite: 'testing.Suite', cls):
        def case():
            """Bound routine representation."""
            LOGGER.debug('Testing %s[%s] case', suite, self._name)
            self._test(suite)

        case.__doc__ = f'Test of {string.capwords(self._name.replace("_", " "))}'
        return case


def operator(subject: type[flow.Operator]) -> type['testing.Suite']:
    """Operator base class generator.

    Args:
        subject: Operator to be tested within given suite.
    """

    class Operator(Suite, metaclass=Meta):
        """Generated base class."""

        @property
        def __operator__(self) -> type[flow.Operator]:
            """Attached operator.

            Returns:
                Operator instance.
            """
            return subject

    return Operator
