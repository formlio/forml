"""
Flow actor abstraction.
"""

import abc
import collections
import inspect
import io
import logging
import pickle
import types
import typing

import joblib

from forml import flow

LOGGER = logging.getLogger(__name__)


class Actor(metaclass=abc.ABCMeta):
    """Abstract interface of an actor.
    """
    @classmethod
    def spec(cls, **params: typing.Any) -> 'Spec':
        """Shortcut for creating a spec of this actor.

        Args:
            **params: Params to be used for the spec.

        Returns: Actor spec instance.
        """
        return Spec(cls, **params)

    @classmethod
    def is_stateful(cls) -> bool:
        """Check whether this actor is stateful (determined based on existence user-overridden train method).

        Returns: True if stateful.
        """
        return cls.train.__code__ is not Actor.train.__code__

    def train(self, features: typing.Any, label: typing.Any) -> None:  # pylint: disable=no-self-use
        """Train the actor using the provided features and label.

        Args:
            features: Table of feature vectors.
            label: Table of labels.
        """
        raise RuntimeError('Stateless actor')

    @abc.abstractmethod
    def apply(self, *features: typing.Any) -> typing.Union[typing.Any, typing.Sequence[typing.Any]]:
        """Pass features through the apply function (typically transform or predict).

        Args:
            features: Table(s) of feature vectors.

        Returns: Transformed features (ie predictions).
        """

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Get hyper-parameters of this actor.

        Default implementation is poor-mans init inspection and is expected to be overridden if not suitable.
        """
        return {p.name: p.default for p in inspect.signature(self.__class__).parameters.values() if
                p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD and p.default is not inspect.Parameter.empty}

    def set_params(self, **params: typing.Any) -> None:
        """Set hyper-parameters of this actor.

        Args:
            params: Dictionary of hyper parameters.
        """
        if params:
            raise RuntimeError(f'Params setter for {params} not implemented on {self.__class__.__name__}')

    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        Returns: state as bytes.
        """
        if not self.is_stateful():
            return bytes()
        LOGGER.debug('Getting %s state', self)
        with io.BytesIO() as bio:
            joblib.dump(self.__dict__, bio, protocol=pickle.HIGHEST_PROTOCOL)
            return bio.getvalue()

    def set_state(self, state: bytes) -> None:
        """Set new internal state of the actor. Note this doesn't change the setting of the actor hyper-parameters.

        Args:
            state: bytes to be used as internal state.
        """
        if not state:
            return
        if not self.is_stateful():
            raise flow.Error('State provided but actor stateless')
        LOGGER.debug('Setting %s state (%d bytes)', self, len(state))
        params = self.get_params()  # keep the original hyper-params
        with io.BytesIO(state) as bio:
            self.__dict__.update(joblib.load(bio))
        self.set_params(**params)  # restore the original hyper-params

    def __str__(self):
        return self.__class__.__name__


class Spec(collections.namedtuple('Spec', 'actor, params')):
    """Wrapper of actor class and init params.
    """
    def __new__(cls, actor: typing.Type[Actor], **params: typing.Any):
        return super().__new__(cls, actor, types.MappingProxyType(params))

    def __str__(self):
        return self.actor.__name__ if inspect.isclass(self.actor) else str(self.actor)

    def __hash__(self):
        return hash(self.actor) ^ hash(tuple(sorted(self.params.items())))

    def __getnewargs_ex__(self):
        return (self.actor, ), dict(self.params)

    def __call__(self, **kwargs) -> Actor:
        return self.actor(**{**self.params, **kwargs})
