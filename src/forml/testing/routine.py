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
import logging
import string
import typing
import unittest

from forml import runtime
from forml.conf import provider as provcfg
from forml.flow.pipeline import topology
from forml.testing import spec, facility

LOGGER = logging.getLogger(__name__)


class Suite(unittest.TestCase, metaclass=abc.ABCMeta):
    """Abstract base class of operator testing suite.
    """
    def __repr__(self):
        return self.__class__.__name__

    @property
    @abc.abstractmethod
    def __operator__(self) -> typing.Type[topology.Operator]:
        """Operator instance.
        """


class Meta(abc.ABCMeta):
    """Meta class for generating unittest classes out of our framework.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any], **kwargs):
        if not any(issubclass(b, Suite) for b in bases):
            raise TypeError(f'{name} not a valid {Suite.__name__}')
        for title, scenario in [(t, s) for t, s in namespace.items() if isinstance(s, spec.Scenario)]:
            namespace[f'test_{title}'] = Case(title, scenario)
            del namespace[title]
        return super().__new__(mcs, name, bases, namespace)


class Test:
    """Base class for test implementations.
    """
    def __init__(self, runner: facility.Runner):
        self._runner: facility.Runner = runner

    def __call__(self, suite: Suite) -> None:
        runner = self.init(suite)
        if runner:
            with self.raises(suite):
                self.matches(suite, self.test(runner))

    def init(self, suite: Suite) -> runtime.Runner:
        """Test init phase.

        Args:
            suite: Testing suite.

        Returns: Runner instance.
        """
        return self._runner(suite.__operator__)

    def raises(self, suite: Suite) -> typing.ContextManager:  # pylint: disable=unused-argument, no-self-use
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.

        Returns: an assertRaises context manager.
        """
        return contextlib.suppress()  # or contextlib.nullcontext() in python3.7

    def matches(self, suite: Suite, value: typing.Any) -> None:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.
            value: Tested value..
        """

    def test(self, runner: runtime.Runner) -> typing.Any:
        """Test subject logic.

        Args:
            runner: Runner instance.

        Returns: Produced value.
        """


class RaisableTest(Test):
    """Base test class for raising test cases.
    """
    def __init__(self, runner: facility.Runner, exception: spec.Scenario.Exception):
        super().__init__(runner)
        self._exception: spec.Scenario.Exception = exception

    def raises(self, suite: Suite) -> typing.ContextManager:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.

        Returns: an assertRaises context manager.
        """
        if self._exception.message is not None:
            return suite.assertRaisesRegex(self._exception.kind, self._exception.message)
        return suite.assertRaises(self._exception.kind)


class ReturnableTest(Test):
    """Base test class for returning test cases.
    """
    def __init__(self, runner: facility.Runner, output: spec.Scenario.Output):
        super().__init__(runner)
        self._output: spec.Scenario.Output = output

    def matches(self, suite: Suite, value: typing.Any) -> None:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.
            value: Tested value.
        """
        if self._output.matcher is not None:
            suite.assertTrue(self._output.matcher(self._output.value, value))
        else:
            suite.assertEqual(self._output.value, value)


class TestInitRaises(RaisableTest, Test):
    """Test composite.
    """
    def init(self, suite: Suite) -> runtime.Runner:
        with self.raises(suite):
            return super().init(suite)


class PlainApplyTest(Test):
    """Testcase logic.
    """
    def test(self, runner: runtime.Runner) -> typing.Any:
        return runner.apply()


class TestPlainApplyReturns(ReturnableTest, PlainApplyTest):
    """Test composite.
    """


class TestPlainApplyRaises(RaisableTest, PlainApplyTest):
    """Test composite.
    """


class StateTrainTest(Test):
    """Testcase logic.
    """
    def test(self, runner: runtime.Runner) -> typing.Any:
        return runner.train()


class TestStateTrainReturns(ReturnableTest, StateTrainTest):
    """Test composite.
    """


class TestStateTrainRaises(RaisableTest, StateTrainTest):
    """Test composite.
    """


class StateApplyTest(Test):
    """Testcase logic.
    """
    def init(self, suite: Suite) -> runtime.Runner:
        runner = super().init(suite)
        runner.train()
        return runner

    def test(self, runner: runtime.Runner) -> typing.Any:
        return runner.apply()


class TestStateApplyReturns(ReturnableTest, StateApplyTest):
    """Test composite.
    """


class TestStateApplyRaises(RaisableTest, StateApplyTest):
    """Test composite.
    """


class Case:
    """Test case routine.
    """
    def __init__(self, name: str, scenario: spec.Scenario, runner: provcfg.Runner = provcfg.Runner.default):
        self._name: str = name
        runner = facility.Runner(scenario.params, scenario.input, runner)
        self._test: Test = self.select(scenario, runner)

    @staticmethod
    def select(scenario: spec.Scenario, runner: runtime.Runner) -> Test:
        """Selecting and setting up the test implementation for given scenario.

        Args:
            scenario: Testing scenario specification.
            runner: Test runner with given operator.

        Returns: Test case instance.
        """
        if scenario.outcome is spec.Scenario.Outcome.INIT_RAISES:
            return TestInitRaises(runner, scenario.exception)
        if scenario.outcome is spec.Scenario.Outcome.PLAINAPPLY_RAISES:
            return TestPlainApplyRaises(runner, scenario.exception)
        if scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RAISES:
            return TestStateTrainRaises(runner, scenario.exception)
        if scenario.outcome is spec.Scenario.Outcome.STATEAPPLY_RAISES:
            return TestStateApplyRaises(runner, scenario.exception)
        if scenario.outcome is spec.Scenario.Outcome.PLAINAPPLY_RETURNS:
            return TestPlainApplyReturns(runner, scenario.output)
        if scenario.outcome is spec.Scenario.Outcome.STATETRAIN_RETURNS:
            return TestStateTrainReturns(runner, scenario.output)
        if scenario.outcome is spec.Scenario.Outcome.STATEAPPLY_RETURNS:
            return TestStateApplyReturns(runner, scenario.output)
        raise RuntimeError('Unexpected scenario outcome')

    def __get__(self, suite: Suite, cls):
        def case():
            """Bound routine representation.
            """
            LOGGER.debug('Testing %s[%s] case', suite, self._name)
            self._test(suite)
        case.__doc__ = f'Test of {string.capwords(self._name.replace("_", " "))}'
        return case
