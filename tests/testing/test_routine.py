"""
ForML testing routine unit tests.
"""
# pylint: disable=protected-access,no-self-use

import typing

from forml.testing import spec


class Runner:
    """Runner mock.
    """
    class Outcome:
        """Runner outcome mock
        """
        def __init__(self, init: typing.Optional[spec.Scenario.Exception] = None,
                     apply: typing.Any = None, train: typing.Any = None):
            if init is not None:
                self._raise(init)
            self._apply: typing.Any = apply
            self._train: typing.Any = train

        @staticmethod
        def _raise(exception: spec.Scenario.Exception):
            """Helper for raising exception.
            """
            raise exception.kind(exception.message)

        def train(self):
            """Runner train mode mock.
            """
            if isinstance(self._train, spec.Scenario.Exception):
                self._raise(self._train)
            return self._train

        def apply(self):
            """Runner apply mode mock.
            """
            if isinstance(self._apply, spec.Scenario.Exception):
                self._raise(self._apply)
            return self._apply

    def __init__(self, scenario: spec.Scenario):
        self._scenario: spec.Scenario = scenario

    def __call__(self, _) -> 'Runner.Outcome':
        if self._scenario is spec.Scenario.Outcome.INIT_RAISES:
            return self.Outcome(init=self._scenario.exception)
        if self._scenario in {spec.Scenario.Outcome.PLAINAPPLY_RAISES, spec.Scenario.Outcome.STATEAPPLY_RAISES}:
            return self.Outcome(apply=self._scenario.exception)
        if self._scenario in {spec.Scenario.Outcome.PLAINAPPLY_RETURNS, spec.Scenario.Outcome.STATEAPPLY_RETURNS}:
            return self.Outcome(apply=self._scenario.output.apply)
        if self._scenario is spec.Scenario.Outcome.STATETRAIN_RAISES:
            return self.Outcome(train=self._scenario.exception)
        if self._scenario is spec.Scenario.Outcome.STATETRAIN_RETURNS:
            return self.Outcome(train=self._scenario.output.train)
        raise RuntimeError('Invalid scenario')
