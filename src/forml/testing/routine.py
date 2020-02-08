"""
Testing routines.
"""
import contextlib
import logging
import string
import typing

from forml import conf, etl
from forml.etl import expression
from forml.flow import task
from forml.flow.pipeline import topology
from forml.runtime import process
from forml.stdlib.operator import simple
from forml.testing import spec

LOGGER = logging.getLogger(__name__)


class Runner:
    """Test runner is a minimal forml pipeline wrapping the tested operator.
    """
    @simple.Labeler.operator
    class Extractor(task.Actor):
        """Just split the features-label pair.
        """

        def apply(self, bundle: typing.Tuple[typing.Any, typing.Any]) -> typing.Tuple[typing.Any, typing.Any]:
            return bundle

    def __init__(self, params: 'spec.Scenario.Params', scenario: 'spec.Scenario.Input',
                 runner: typing.Optional[str] = conf.TESTING_RUNNER):
        self._params: spec.Scenario.Params = params
        self._source: etl.Source = etl.Extract(expression.Select(lambda: (scenario.train, scenario.label)),
                                               expression.Select(lambda: scenario.apply)) >> Runner.Extractor()
        self._runner: typing.Optional[str] = runner

    def __call__(self, operator: typing.Type[topology.Operator]) -> process.Runner:
        return self._source.bind(operator(*self._params.args, **self._params.kwargs)).launcher[self._runner]


class Test:
    """Base class for test implementations.
    """
    def __init__(self, runner: Runner):
        self._runner: Runner = runner

    def __call__(self, suite: 'spec.Suite') -> None:
        runner = self.init(suite)
        with self.raises(suite):
            self.matches(suite, self.test(runner))

    def init(self, suite: 'spec.Suite') -> process.Runner:
        """Test init phase.

        Args:
            suite: Testing suite.

        Returns: Runner instance.
        """
        return self._runner(suite.__operator__)

    def raises(self, suite: 'spec.Suite') -> typing.ContextManager:  # pylint: disable=unused-argument, no-self-use
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.

        Returns: an assertRaises context manager.
        """
        return contextlib.suppress()  # or contextlib.nullcontext() in python3.7

    def matches(self, suite: 'spec.Suite', value: typing.Any) -> None:
        """Context manager for wrapping raising assertions.

        Args:
            suite: Tested suite.
            value: Tested value..
        """

    def test(self, runner: process.Runner) -> typing.Any:
        """Test subject logic.

        Args:
            runner: Runner instance.

        Returns: Produced value.
        """


class RaisableTest(Test):
    """Base test class for raising test cases.
    """
    def __init__(self, runner: Runner, exception: 'spec.Scenario.Exception'):
        super().__init__(runner)
        self._exception: spec.Scenario.Exception = exception

    def raises(self, suite: 'spec.Suite') -> typing.ContextManager:
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
    def __init__(self, runner: Runner, output: 'spec.Scenario.Output'):
        super().__init__(runner)
        self._output: spec.Scenario.Output = output

    def matches(self, suite: 'spec.Suite', value: typing.Any) -> None:
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
    def init(self, suite: 'spec.Suite') -> process.Runner:
        with self.raises(suite):
            return super().init(suite)


class PlainApplyTest(Test):
    """Testcase logic.
    """
    def test(self, runner: process.Runner) -> typing.Any:
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
    def test(self, runner: process.Runner) -> typing.Any:
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
    def init(self, suite: 'spec.Suite') -> process.Runner:
        runner = super().init(suite)
        runner.train()
        return runner

    def test(self, runner: process.Runner) -> typing.Any:
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
    def __init__(self, name: str, scenario: 'spec.Scenario', runner: typing.Optional[str] = conf.TESTING_RUNNER):
        self._name: str = name
        self._test: Test = self.select(scenario, runner)

    @staticmethod
    def select(scenario: 'spec.Scenario', runner: typing.Optional[str]) -> Test:
        """Selecting and setting up the test implementation for given scenario.

        Args:
            scenario: Testing scenario specification.
            runner: Test runner with given operator.

        Returns: Test case instance.
        """
        runner = Runner(scenario.params, scenario.input, runner)
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

    def __get__(self, suite: 'spec.Suite', cls):
        def case():
            """Bound routine representation.
            """
            LOGGER.debug('Testing %s[%s] case', suite, self._name)
            self._test(suite)
        case.__doc__ = f'Test of {string.capwords(self._name.replace("_", " "))}'
        return case
