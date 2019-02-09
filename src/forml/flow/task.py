"""
Flow actor abstraction.
"""

import abc
import collections
import inspect
import io
import pickle
import types
import typing

import joblib

DataT = typing.TypeVar('DataT')


class Actor(typing.Generic[DataT], metaclass=abc.ABCMeta):
    """Abstract interface of an actor.
    """
    @classmethod
    def is_stateful(cls) -> bool:
        """Check whether this actor is stateful (determined based on existence user-overridden train method).

        Returns: True if stateful.
        """
        return cls.train.__code__ is not Actor.train.__code__

    def train(self, features: DataT, label: DataT) -> None:
        """Train the actor using the provided features and label.

        Args:
            features: Table of feature vectors.
            label: Table of labels.
        """
        raise NotImplementedError('Stateless actor')

    @abc.abstractmethod
    def apply(self,
              features: typing.Union[DataT, typing.Sequence[DataT]]) -> typing.Union[DataT, typing.Sequence[DataT]]:
        """Pass features through the apply function (typically transform or predict).

        Args:
            features: Table(s) of feature vectors.

        Returns: Transformed features (ie predictions).
        """

    @abc.abstractmethod
    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Get hyper-parameters of this actor.
        """
        # TODO: is get_params necessary? should be rather static to provide list of params for validation?

    @abc.abstractmethod
    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Set hyper-parameters of this actor.

        Args:
            params: Dictionary of hyper parameters.
        """
        # TODO: is set_params necessary?

    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        Returns: state as bytes.
        """
        if not self.is_stateful():
            return bytes()
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
        assert self.is_stateful(), 'State provided but actor stateless'
        with io.BytesIO(state) as bio:
            self.__dict__.update(joblib.load(bio))

    def __str__(self):
        return self.__class__.__name__


class Spec(collections.namedtuple('Spec', 'actor, params')):
    """Wrapper of actor class and init params.
    """
    def __new__(cls, actor: typing.Type[Actor], **params: typing.Any):
        return super().__new__(cls, actor, types.MappingProxyType(params))

    def __str__(self):
        return self.actor.__name__ if inspect.isclass(self.actor) else str(self.actor)

    def __getnewargs_ex__(self):
        return (self.actor, ), dict(self.params)

    def __hash__(self):
        return hash(self.actor) ^ hash(tuple(sorted(self.params.items())))


class Wrapped:
    """Decorator wrapper.
    """
    class Actor(Actor):  # pylint: disable=abstract-method
        """Wrapper around user class implementing the Actor interface.
        """
        def __new__(cls, actor: typing.Any, mapping: typing.Mapping[str, str]):  # pylint: disable=unused-argument
            cls.__abstractmethods__ = frozenset()
            return super().__new__(cls)

        def __init__(self, actor: typing.Any, mapping: typing.Mapping[str, str]):
            super().__init__()
            self._actor: typing.Any = actor
            self._mapping: typing.Mapping[str, str] = mapping

        def __getnewargs__(self):
            return self._actor, self._mapping

        def __getattribute__(self, item):
            if item.startswith('_') or item not in self._mapping and not hasattr(self._actor, item):
                return super().__getattribute__(item)
            return getattr(self._actor, self._mapping.get(item, item))

    def __init__(self, actor: typing.Type, mapping: typing.Mapping[str, str]):
        assert not issubclass(actor, Actor), 'Wrapping a true actor'
        self._actor: typing.Type = actor
        self._mapping: typing.Mapping[str, str] = mapping

    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns: True if the wrapped actor is stateful (has a train method).
        """
        return hasattr(self._actor, self._mapping[Actor.train.__name__])

    def __str__(self):
        return str(self._actor.__name__)

    def __call__(self, *args, **kwargs) -> Actor:
        return self.Actor(self._actor(*args, **kwargs), self._mapping)  # pylint: disable=abstract-class-instantiated

    def __hash__(self):
        return hash(self._actor) ^ hash(tuple(sorted(self._mapping.items())))

    def __eq__(self, other: typing.Any):
        # pylint: disable=protected-access
        return isinstance(other, self.__class__) and self._actor == other._actor and self._mapping == other._mapping

    @staticmethod
    def actor(cls: typing.Optional[typing.Type] = None, **mapping: str):  # pylint: disable=bad-staticmethod-argument
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
            for target in {t for s, t in mapping.items() if s != Actor.train.__name__}:
                assert callable(getattr(cls, target, None)), f'Wrapped actor missing required {target} implementation'
            return Wrapped(cls, mapping)

        if cls:
            decorator = decorator(cls)
        return decorator
