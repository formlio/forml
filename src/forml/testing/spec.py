"""
Testing specification elements.
"""
import abc
import collections
import types
import typing
import unittest

from forml.flow.pipeline import topology
from forml.testing import routine


class Scenario(collections.namedtuple('Scenario', 'params, input, output, exception')):
    """Test case specification.
    """
    class Params(collections.namedtuple('Params', 'args, kwargs')):
        """Operator hyper-parameters.
        """
        def __new__(cls, *args: typing.Any, **kwargs: typing.Any):
            return super().__new__(cls, args, types.MappingProxyType(kwargs))

    class Data(collections.namedtuple('Data', 'apply, train, label')):
        """Input/output datasets.
        """
        def __new__(cls, apply: typing.Any = None, train: typing.Any = None, label: typing.Any = None):
            return super().__new__(cls, apply, train, label)

        def __bool__(self):
            return self.applied or self.trained

        @property
        def applied(self) -> bool:
            """Test this is a (to-be) applied in/output.
            """
            return self.apply is not None

        @property
        def trained(self) -> bool:
            """Test this is a (to-be) trained in/output.
            """
            return self.train is not None or self.label is not None

    def __new__(cls, params: 'Scenario.Params', input: typing.Optional['Scenario.Data'] = None,
                output: typing.Optional['Scenario.Data'] = None,
                exception: typing.Optional[typing.Type[Exception]] = None):
        if not input:
            input = cls.Data()
        if not output:
            output = cls.Data()
        if not bool(output) ^ bool(exception):
            raise ValueError('Exclusive outcome required')
        if output and not input:
            raise ValueError('Output without input')
        if output.applied and output.trained:
            raise ValueError('Output apply/train collision')
        return super().__new__(cls, params, input, output, exception)

    @property
    def applied(self) -> bool:
        return self.input.applied

    @property
    def trained(self) -> bool:
        return self.input.trained

    @property
    def raises(self) -> bool:
        return self.exception is not None


class Raisable:
    """Base outcome type allowing a raising assertion.
    """
    def __init__(self, params: 'Scenario.Params', input: typing.Optional['Scenario.Data'] = None):
        self._params: Scenario.Params = params
        self._input: Scenario.Data = input or Scenario.Data()

    def raises(self, exception: typing.Type[Exception]) -> Scenario:
        """Assertion on expected exception.
        """
        return Scenario(self._params, self._input, exception=exception)


class Applied(Raisable):
    """Outcome with a apply input dataset defined.
    """
    def returns(self, features: typing.Any) -> Scenario:
        """Assertion on expected return value.
        """
        return Scenario(self._params, self._input, Scenario.Data(apply=features))


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
    def returns(self, features: typing.Any, labels: typing.Any = None) -> Scenario:
        """Assertion on expected return value.
        """
        return Scenario(self._params, self._input, Scenario.Data(train=features, label=labels))


class Case(Appliable):
    """Test case entrypoint.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(Scenario.Params(*args, **kwargs))

    def train(self, features: typing.Any, labels: typing.Any) -> Trained:
        """Train input dataset definition.
        """
        return Trained(self._params, Scenario.Data(train=features, label=labels))


class Suite(unittest.TestCase, metaclass=abc.ABCMeta):
    """Abstract base class of operator testing suite.
    """
    @property
    @abc.abstractmethod
    def __operator__(self) -> typing.Type[topology.Operator]:
        """Operator instance.
        """


class Meta(abc.ABCMeta):
    """Meta class for generating unittest classes out of our framework.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]):
        if not any(issubclass(b, Suite) for b in bases):
            raise TypeError(f'{name} not a valid {Suite.__name__}')
        for title, scenario in [(t, s) for t, s in namespace.items() if isinstance(s, Scenario)]:
            def case(suite: Suite) -> None:
                routine.case(title, suite.__operator__, scenario)
            case.__doc__ = f'Test case for {title}'
            namespace[f'test{title}'] = case
            del namespace[title]
        return super().__new__(mcs, name, bases, namespace)
