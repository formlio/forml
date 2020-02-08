"""
Testing routines.
"""
import logging
import typing

from forml.flow import task

from forml.stdlib.operator import simple

from forml.etl import expression

from forml import conf, etl, project as prjmod
from forml.flow.pipeline import topology
from forml.testing import spec

LOGGER = logging.getLogger(__name__)


@simple.Labeler.operator
class Extractor(task.Actor):
    """Just split the features-label pair.
    """
    def apply(self, bundle: typing.Tuple[typing.Any, typing.Any]) -> typing.Tuple[typing.Any, typing.Any]:
        return bundle


def project(operator: typing.Type[topology.Operator], scenario: 'spec.Scenario') -> prjmod.Artifact:
    """Create project artifact for tested operator.

    Args:
        operator: Tested operator type.
        scenario: Testing scenario.

    Returns: Project artifact.
    """
    source = etl.Extract(expression.Select(lambda: (scenario.input.train, scenario.input.label)),
                         expression.Select(lambda: scenario.input.apply)) >> Extractor()
    return source.bind(operator(*scenario.params.args, **scenario.params.kwargs))


def run(suite: 'spec.Suite', case: str, operator: typing.Type[topology.Operator], scenario: 'spec.Scenario',
        runner: typing.Optional[str] = conf.TESTING_RUNNER) -> None:
    """Run the test case based on the given scenario.

    Args:
        suite: Test suite instance.
        case: Test case name.
        operator: Operator type.
        scenario: Test scenario descriptor.
        runner: Runner implementation.
    """
    LOGGER.debug('Testing %s[%s] case', suite, case)
    if scenario.outcome == spec.Scenario.Outcome.INIT_RAISES:
        with suite.assertRaises(scenario.exception):
            operator(*scenario.params.args, **scenario.params.kwargs)
            return
    launcher = project(operator, scenario).launcher[runner]
    if scenario.outcome == spec.Scenario.Outcome.PLAINAPPLY_RAISES:
        with suite.assertRaises(scenario.exception):
            launcher.apply()
    elif scenario.outcome == spec.Scenario.Outcome.STATETRAIN_RAISES:
        with suite.assertRaises(scenario.exception):
            launcher.train()
    elif scenario.outcome == spec.Scenario.Outcome.STATEAPPLY_RAISES:
        launcher.train()
        with suite.assertRaises(scenario.exception):
            launcher.apply()
    elif scenario.outcome == spec.Scenario.Outcome.PLAINAPPLY_RETURNS:
        suite.assertEqual(launcher.apply(), scenario.output.apply)
    elif scenario.outcome == spec.Scenario.Outcome.STATETRAIN_RETURNS:
        suite.assertEqual(launcher.train(), scenario.output.train)
    elif scenario.outcome == spec.Scenario.Outcome.STATEAPPLY_RETURNS:
        launcher.train()
        suite.assertEqual(launcher.apply(), scenario.output.apply)
    else:
        raise RuntimeError('Unexpected scenario outcome')


def generate(case: str, scenario: 'spec.Scenario') -> typing.Callable[['spec.Suite'], None]:
    """Closure for generating the test case routine.

    Args:
        case: Test case title.
        scenario: Test scenario descriptor.

    Returns: Test case routine.
    """
    def routine(suite: 'spec.Suite') -> None:
        """Test case routine.

        Args:
            suite: suite to test on.
        """
        run(suite, case, suite.__operator__, scenario)
    routine.__doc__ = f'Test case routine for {case}.'
    return routine
