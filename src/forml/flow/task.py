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
    @abc.abstractmethod
    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the actor using the provided features and label.

        Args:
            features: Table of feature vectors.
            label: Table of labels.
        """

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
        with io.BytesIO() as bio:
            joblib.dump(self.__dict__, bio, protocol=pickle.HIGHEST_PROTOCOL)
            return bio.getvalue()

    def set_state(self, state: bytes) -> None:
        """Set new internal state of the actor.

        Args:
            state: bytes to be used as internal state.
        """
        with io.BytesIO(state) as bio:
            self.__dict__.update(joblib.load(bio))

    def __getstate__(self):
        return self.get_state()

    def __setstate__(self, state: bytes):
        self.set_state(state)


class Spec(collections.namedtuple('Spec', 'actor, params')):
    """Wrapper of actor class and init params.
    """
    def __new__(cls, actor: Actor, params: typing.Dict[str, typing.Any]):
        return super().__new__(cls, actor, types.MappingProxyType(params))

    def __getnewargs__(self):
        return self.actor, dict(self.params)

    def __hash__(self):
        return hash(self.actor) ^ hash(tuple(sorted(self.items())))


def actor(cls: typing.Optional[typing.Type] = None, **mapping):
    """Decorator for turning an user class to a valid actor. This can be used either as parameterless decorator or
    optionally with mapping of Actor methods to decorated user class implementation.

    Args:
        cls: Decorated class.
        apply: Name of user class method implementing the actor apply.
        train: Name of user class method implementing the actor train.
        get_params: Name of user class method implementing the actor get_params.
        set_params: Name of user class method implementing the actor set_params.
        get_state: Name of user class method implementing the actor get_state.
        set_state: Name of user class method implementing the actor set_statte.

    Returns: Actor class.
    """
    assert bool(cls) ^ bool(mapping), 'Unexpected positional argument provided together with mapping'
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
        class Decorated(Actor):
            """Wrapper around user class implementing the Actor interface.
            """
            def __new__(cls):
                cls.__abstractmethods__ = frozenset()
                return super().__new__(cls)

            def __init__(self, *args, **kwargs):
                self._actor = cls(*args, **kwargs)

            def __getattr__(self, item):
                return getattr(self._actor, mapping.get(item, item))

        return Decorated

    if cls:  # used as paramless decorator
        decorator = decorator(cls)
    return decorator
