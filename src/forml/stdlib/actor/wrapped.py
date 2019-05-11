"""
Utilities for creating actors using decorator wrappings.
"""

import abc
import inspect
import typing

from forml import stdlib
from forml.flow import task


class Error(stdlib.Error):
    """Actor error.
    """


class Wrapping(metaclass=abc.ABCMeta):
    """Base class for wrappers.
    """
    def __init__(self, actor: typing.Type, params: typing.Mapping[str, str]):
        self._actor: typing.Any = actor
        self._params: typing.Mapping[str, str] = params

    def __hash__(self):
        return hash(self._actor) ^ hash(tuple(sorted(self._params.items())))

    def __eq__(self, other: typing.Any):
        # pylint: disable=protected-access
        return isinstance(other, self.__class__) and self._actor == other._actor and self._params == other._params

    def __str__(self):
        cls = self._actor
        if not inspect.isclass(cls):
            cls = type(cls)
        return cls.__name__

    @abc.abstractmethod
    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns: True if the wrapped actor is stateful (has a train method).
        """

    def spec(self, **params) -> task.Spec:
        """Shortcut for creating a spec of this actor.

        Args:
            **params: Params to be used for the spec.

        Returns: Actor spec instance.
        """
        return task.Spec(self, **params)


class Mapping(Wrapping):
    """Base class for actor wrapping.
    """
    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns: True if the wrapped actor is stateful (has a train method).
        """
        return hasattr(self._actor, self._params[task.Actor.train.__name__])


class Class(Mapping):
    """Decorator wrapper.
    """
    class Actor(Mapping, task.Actor):  # pylint: disable=abstract-method
        """Wrapper around user class implementing the Actor interface.
        """
        def __new__(cls, actor: typing.Any, mapping: typing.Mapping[str, str]):  # pylint: disable=unused-argument
            cls.__abstractmethods__ = frozenset()
            return super().__new__(cls)

        def __init__(self, actor: typing.Any, params: typing.Mapping[str, str]):
            super().__init__(actor, params)

        def __getnewargs__(self):
            return self._actor, self._params

        def __getattribute__(self, item):
            if item.startswith('_') or item not in self._params and not hasattr(self._actor, item):
                return super().__getattribute__(item)
            return getattr(self._actor, self._params.get(item, item))

    def __init__(self, actor: typing.Type, params: typing.Mapping[str, str]):
        assert not issubclass(actor, task.Actor), 'Wrapping a true actor'
        super().__init__(actor, params)

    def __call__(self, *args, **kwargs) -> task.Actor:
        return self.Actor(self._actor(*args, **kwargs), self._params)  # pylint: disable=abstract-class-instantiated

    @staticmethod
    def actor(cls: typing.Optional[typing.Type] = None,  # pylint: disable=bad-staticmethod-argument
              **mapping: str) -> typing.Type[task.Actor]:
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
        if not all(isinstance(a, str) for a in mapping.values()):
            raise ValueError('Invalid mapping')

        for method in (task.Actor.apply, task.Actor.train, task.Actor.get_params, task.Actor.set_params):
            mapping.setdefault(method.__name__, method.__name__)

        def decorator(cls) -> typing.Type[task.Actor]:
            """Decorating function.
            """
            if not inspect.isclass(cls):
                raise ValueError(f'Invalid actor class {cls}')
            if issubclass(cls, task.Actor):
                return cls
            for target in {t for s, t in mapping.items() if s != task.Actor.train.__name__}:
                if not callable(getattr(cls, target, None)):
                    raise Error(f'Wrapped actor missing required {target} implementation')
            return Class(cls, mapping)

        if cls:
            decorator = decorator(cls)
        return decorator


class Function(Wrapping):
    """Function wrapping actor.
    """
    class Actor(task.Actor):
        """Wrapper around user class implementing the Actor interface.
        """
        def __init__(self, function: typing.Callable[[typing.Any], typing.Any],
                     **kwargs: typing.Mapping[str, typing.Any]):
            self._function: typing.Callable[[typing.Any], typing.Any] = function
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __str__(self):
            return self._function.__name__

        def apply(self, *features: typing.Any) -> typing.Union[typing.Any, typing.Sequence[typing.Any]]:
            return self._function(*features, **self._kwargs)

        def get_params(self) -> typing.Dict[str, typing.Any]:
            """Standard param getter.

            Returns: Evaluation function.
            """
            return {'function': self._function, 'kwargs': dict(self._kwargs)}

        def set_params(self,  # pylint: disable=arguments-differ
                       function: typing.Optional[typing.Callable[[typing.Any], float]] = None,
                       kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None) -> None:
            """Standard params setter.

            Args:
                function: Evaluation function.
                kwargs: Extra kwargs to be passed to function.
            """
            if function:
                self._function = function
            if kwargs:
                self._kwargs = kwargs

    def __init__(self, function: typing.Callable[[typing.Any], typing.Any], **kwargs: typing.Mapping[str, typing.Any]):
        super().__init__(function, kwargs)

    def __call__(self, **kwargs) -> 'Function.Actor':
        return self.Actor(self._actor, **{**self._params, **kwargs})

    def __str__(self):
        return str(self._actor)

    def is_stateful(self) -> bool:
        """Wrapped function is generally stateless.

        Returns: False
        """
        return False

    @staticmethod
    def actor(function: typing.Optional[typing.Callable[[typing.Any], typing.Any]] = None,
              **kwargs: typing.Any) -> typing.Type[task.Actor]:
        """Decorator for turning an user function to a valid actor. This can be used either as parameterless decorator
        or optionally with addition kwargs that will be passed to the function.

        Args:
            function: Decorated function.
            kwargs: Optional kwargs to be passed to function.

        Returns: Actor class.
        """

        def decorator(function) -> typing.Type[task.Actor]:
            """Decorating function.
            """
            if not inspect.isfunction(function):
                raise ValueError(f'Invalid actor function {function}')
            return Function(function, **kwargs)

        if function:
            decorator = decorator(function)
        return decorator