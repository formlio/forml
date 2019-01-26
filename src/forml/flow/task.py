"""

@Transformer  # wrap as actor capturing init args returning a Transformer operator
class NaImputer:
    def __init__(self, x, y):
        ...

    def fit(self, x, y):
        ...

    def transform(self, x):
        ...

"""

import abc
import collections
import inspect
import io
import pickle
import types
import typing

import joblib
import pandas


class Actor(metaclass=abc.ABCMeta):
    """Abstract interface of an actor.
    """
    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the actor using the provided features and label.

        Args:
            features: Table of feature vectors.
            label: Table of labels.
        """
        raise NotImplementedError('Stateless actor')

    @abc.abstractmethod
    def apply(self, *features: pandas.DataFrame) -> typing.Sequence[pandas.DataFrame]:
        """Pass features through the apply function (typically transform or predict).

        Args:
            features: Table of feature vectors.

        Returns: Transformed features (ie predictions).
        """

    @abc.abstractmethod
    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Get hyper-parameters of this actor.
        """

    @abc.abstractmethod
    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Set hyper-parameters of this actor.

        Args:
            params: Dictionary of hyper parameters.
        """

    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        Returns: state as bytes.
        """
        # if self.train.__code__ is Actor.train.__code__:  # Train not overridden - stateless actor
        #     return bytes()
        with io.BytesIO() as bio:
            joblib.dump(self.__dict__, bio, protocol=pickle.HIGHEST_PROTOCOL)
            return bio.getvalue()

    def set_state(self, state: bytes) -> None:
        """Set new internal state of the actor.

        Args:
            state: bytes to be used as internal state.
        """
        if not state:
            return
        with io.BytesIO(state) as bio:
            self.__dict__.update(joblib.load(bio))


class Spec(collections.namedtuple('Spec', 'actor, params')):
    """Wrapper of actor class and init params.
    """
    def __new__(cls, actor: Actor, params: typing.Dict[str, typing.Any]):
        return super().__new__(cls, actor, types.MappingProxyType(params))

    def __getnewargs__(self):
        return self.actor, dict(self.params)

    def __hash__(self):
        return hash(self.actor) ^ hash(tuple(sorted(self.params.items())))


class Wrapped:
    """Decorator wrapper.
    """
    class Actor(Actor):
        """Wrapper around user class implementing the Actor interface.
        """
        # pylint: disable=abstract-method
        def __new__(cls, actor: typing.Any, mapping: typing.Mapping[str, str]):  # pylint: disable=unused-argument
            cls.__abstractmethods__ = frozenset()
            return super().__new__(cls)

        def __init__(self, actor: typing.Any, mapping: typing.Mapping[str, str]):
            self._actor: typing.Any = actor
            self._mapping: typing.Mapping[str, str] = mapping

        def __getnewargs__(self):
            return self._actor, self._mapping

        def __getattribute__(self, item):
            if item.startswith('_') or item not in self._mapping and not hasattr(self._actor, item):
                return super().__getattribute__(item)
            return getattr(self._actor, self._mapping.get(item, item))

    def __init__(self, actor: typing.Type, mapping: typing.Mapping[str, str]):
        self._actor: typing.Any = actor
        self._mapping: typing.Mapping[str, str] = mapping

    def __call__(self, *args, **kwargs):
        return self.Actor(self._actor(*args, **kwargs), self._mapping)  # pylint: disable=abstract-class-instantiated

    def __hash__(self):
        return hash(self._actor) ^ hash(tuple(sorted(self._mapping.items())))

    def __eq__(self, other: typing.Any):
        # pylint: disable=protected-access
        return isinstance(other, self.__class__) and self._actor == other._actor and self._mapping == other._mapping

    @staticmethod
    def actor(cls: typing.Optional[typing.Type] = None, **mapping):  # pylint: disable=bad-staticmethod-argument
        """Decorator for turning an user class to a valid actor. This can be used either as parameterless decorator or
        optionally with mapping of Actor methods to decorated user class implementation.

        Args:
            cls: Decorated class.
            apply: Name of user class method implementing the actor apply.
            train: Name of user class method implementing the actor train.
            get_params: Name of user class method implementing the actor get_params.
            set_params: Name of user class method implementing the actor set_params.

        Returns: Actor class.
        """
        assert all(isinstance(a, str) for a in mapping.values()), 'Invalid mapping'

        for method in (Actor.apply, Actor.train, Actor.get_params, Actor.set_params):
            mapping.setdefault(method.__name__, method.__name__)

        def decorator(cls):
            """Decorating function.
            """
            assert cls and inspect.isclass(cls), f'Invalid actor class {cls}'
            if isinstance(cls, Actor):
                return cls

            # TODO: verify class has required target methods
            return Wrapped(cls, mapping)

        if cls:
            decorator = decorator(cls)
        return decorator
