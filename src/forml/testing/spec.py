"""
Testing specification elements.
"""
import abc
import collections
import enum
import functools
import types
import typing
import unittest

from forml.flow.pipeline import topology
from forml.testing import routine


class Scenario(collections.namedtuple('Scenario', 'params, input, output, exception')):
    """Test case specification.
    """
    @enum.unique
    class Outcome(enum.Enum):
        """Possible outcome type.
        """
        INIT_RAISES = 'init-raises'
        PLAINAPPLY_RAISES = 'plainapply-raises'
        PLAINAPPLY_RETURNS = 'plainapply-returns'
        STATETRAIN_RAISES = 'statetrain-raises'
        STATETRAIN_RETURNS = 'statetrain-returns'
        STATEAPPLY_RAISES = 'stateapply-raises'
        STATEAPPLY_RETURNS = 'stateapply-returns'

        @property
        def raises(self) -> bool:
            """True if this outcome is one of the raises.
            """
            return self in {self.INIT_RAISES, self.PLAINAPPLY_RAISES, self.STATEAPPLY_RAISES}

    class Digest(collections.namedtuple('Digest', 'trained, applied, raised')):
        """Scenario combination fingerprint.
        """

    OUTCOME = {
        Digest(False, False, True): Outcome.INIT_RAISES,
        Digest(False, True, True): Outcome.PLAINAPPLY_RAISES,
        Digest(False, True, False): Outcome.PLAINAPPLY_RETURNS,
        Digest(True, False, True): Outcome.STATETRAIN_RAISES,
        Digest(True, False, False): Outcome.STATETRAIN_RETURNS,
        Digest(True, True, True): Outcome.STATEAPPLY_RAISES,
        Digest(True, True, False): Outcome.STATEAPPLY_RETURNS,
    }

    class Params(collections.namedtuple('Params', 'args, kwargs')):
        """Operator hyper-parameters.
        """
        def __new__(cls, *args: typing.Any, **kwargs: typing.Any):
            return super().__new__(cls, args, types.MappingProxyType(kwargs))

        def __hash__(self):
            return hash(self.args) ^ hash(tuple(sorted(self.kwargs.items())))

    class IO(metaclass=abc.ABCMeta):
        """Input/output base class.
        """
        @property
        @abc.abstractmethod
        def apply(self) -> typing.Any:
            """Apply dataset.

            Returns: The apply dataset.
            """

        def __bool__(self):
            return self.applied or self.trained

        @property
        def applied(self) -> bool:
            """Test this is a (to-be) applied in/output.
            """
            return self.apply is not None

        @property
        @abc.abstractmethod
        def trained(self) -> bool:
            """Test this is a to-be trained in/output.
            """

    class Input(collections.namedtuple('Input', 'apply, train, label'), IO):
        """Input data type.
        """
        def __new__(cls, apply: typing.Any = None, train: typing.Any = None, label: typing.Any = None):
            return super().__new__(cls, apply, train, label)

        @property
        def trained(self) -> bool:
            """Test this is a to-be trained input.
            """
            return self.train is not None or self.label is not None

    class Output(collections.namedtuple('Output', 'apply, train'), IO):
        """Output data type.
        """
        def __new__(cls, apply: typing.Any = None, train: typing.Any = None):
            if apply is not None and train is not None:
                raise ValueError('Output apply/train collision')
            return super().__new__(cls, apply, train)

        @property
        def trained(self) -> bool:
            """Test this is a trained output.
            """
            return self.train is not None

    def __new__(cls, params: 'Scenario.Params',
                input: typing.Optional['Scenario.Input'] = None,  # pylint: disable=redefined-builtin
                output: typing.Optional['Scenario.Output'] = None,
                exception: typing.Optional[typing.Type[Exception]] = None):
        if not output:
            output = cls.Output()
        if not input:
            input = cls.Input()
            if output:
                raise ValueError('Output without input')
            if not exception:
                raise ValueError('Unknown outcome')
        return super().__new__(cls, params, input, output, exception)

    @property
    @functools.lru_cache()
    def outcome(self) -> 'Scenario.Outcome':
        """The outcome type of this scenario.

        Returns: Outcome type.
        """
        return self.OUTCOME[self.Digest(self.input.trained, self.input.applied, self.exception is not None)]


class Raisable:
    """Base outcome type allowing a raising assertion.
    """
    def __init__(self, params: 'Scenario.Params',
                 input: typing.Optional['Scenario.IO'] = None):  # pylint: disable=redefined-builtin
        self._params: Scenario.Params = params
        self._input: Scenario.Input = input or Scenario.Input()

    def raises(self, exception: typing.Type[Exception]) -> Scenario:
        """Assertion on expected exception.
        """
        return Scenario(self._params, self._input, exception=exception)


class Applied(Raisable):
    """Outcome with a apply input dataset defined.
    """
    def returns(self, output: typing.Any) -> Scenario:
        """Assertion on expected return value.
        """
        return Scenario(self._params, self._input, Scenario.Output(apply=output))


class Appliable(Raisable):
    """Outcome type allowing to define an apply input dataset.
    """
    def apply(self, features: typing.Any) -> Applied:
        """Apply input dataset definition.
        """
        return Applied(self._params, self._input._replace(apply=features))


class Trained(Appliable):
    """Outcome with a train input dataset defined.
    """
    def returns(self, output: typing.Any) -> Scenario:
        """Assertion on expected return value.
        """
        return Scenario(self._params, self._input, Scenario.Output(train=output))


class Case(Appliable):
    """Test case entrypoint.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(Scenario.Params(*args, **kwargs))

    def train(self, features: typing.Any, labels: typing.Any) -> Trained:
        """Train input dataset definition.
        """
        return Trained(self._params, Scenario.Input(train=features, label=labels))


class Suite(unittest.TestCase, metaclass=abc.ABCMeta):
    """Abstract base class of operator testing suite.
    """
    def __str__(self):
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
        for title, scenario in [(t, s) for t, s in namespace.items() if isinstance(s, Scenario)]:
            namespace[f'test_{title}'] = routine.generate(title, scenario)
            del namespace[title]
        return super().__new__(mcs, name, bases, namespace)
