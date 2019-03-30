import abc
import typing
import uuid

from forml.flow import task
from forml.runtime import code, persistent


class Loader(code.Symbol):
    """Registry based state loader.
    """
    def __init__(self, registry: persistent.Registry, sid: uuid.UUID):
        self._registry: persistent.Registry = registry
        self._sid: uuid.UUID = sid

    def __call__(self) -> bytes:
        return self._registry._read(self._sid)


class Getter(code.Symbol):
    """Extracting single item from a vector.
    """
    def __init__(self, index: int):
        self._index: int = index

    def __call__(self, sequence: typing.Sequence[typing.Any]) -> typing.Any:
        return sequence[self._index]


class Functor(code.Symbol):
    """Special symbol for wrapping task actors.

    Functor object must be serializable.
    """
    class Shifting:
        """Extra functionality to be prepended to the main objective.
        """
        def __init__(self, reducer: typing.Callable[[task.Actor, typing.Any], task.Actor],
                     objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any]):
            self._reducer: typing.Callable[[task.Actor, typing.Any], task.Actor] = reducer
            self._objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any] = objective

        def __call__(self, actor: task.Actor, first: typing.Any, *args: typing.Any) -> typing.Any:
            if first:
                actor = self._reducer(actor, first)
            return self._objective(actor, *args)

        @staticmethod
        def state(actor: task.Actor, state: bytes) -> task.Actor:
            """Predefined shifting for state taking objective.

            Args:
                actor: Target actor to run the objective on.
                state: Actor state to be used.

            Returns: Actor instance.
            """
            actor.set_state(state)
            return actor

        @staticmethod
        def params(actor: task.Actor, params: typing.Mapping[str, typing.Any]) -> task.Actor:
            """Predefined shifting for params taking objective.

            Args:
                actor: Target actor to run the objective on.
                params: Actor params to be used.

            Returns: Actor instance.
            """
            actor.set_params(**params)
            return actor

    def __init__(self, spec: task.Spec,
                 objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any]):
        self._spec: task.Spec = spec
        self._objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any] = objective
        self._instance: typing.Optional[task.Actor] = None  # transient

    def __reduce__(self):
        return Functor, (self._spec, self._objective)

    def shiftby(self, reducer: typing.Callable[[task.Actor, typing.Any], task.Actor]) -> 'Functor':
        """Prepend the functor objective by extra reducer.

        Args:
            reducer: Callable taking the target actor and eating its first argument.

        Returns: New Functor instance with the objective updated.
        """
        return Functor(self._spec, self.Shifting(reducer, self._objective))

    @property
    def _actor(self) -> task.Actor:
        """Internal cached actor instance.

        Returns: Actor instance.
        """
        if not self._instance:
            self._instance = self._spec()
        return self._instance

    def __call__(self, *args) -> typing.Any:
        return self._objective(self._actor, *args)


class Functional(Functor, metaclass=abc.ABCMeta):
    """Base class for mapper and consumer functors.
    """
    def __init__(self, spec: task.Spec):
        super().__init__(spec, self._objective)

    @staticmethod
    @abc.abstractmethod
    def _objective(actor: task.Actor, *args) -> typing.Any:
        """Delegated actor objective.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns: Anything the actor objective returns.
        """


class Mapper(Functional):
    @staticmethod
    def _objective(actor: task.Actor, *args) -> typing.Any:
        """Mapper objective is the apply method.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns: Output of the apply method.
        """
        return actor.apply(*args)


class Consumer(Functional):
    @staticmethod
    def _objective(actor: task.Actor, *args) -> bytes:
        """Consumer objective is the train method.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns: New actor state.
        """
        actor.train(*args)
        return actor.get_state()

