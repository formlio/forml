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
import types
import typing

import pandas


class Params(collections.Mapping):
    """Hyper-parameters wrapper.
    """
    def __init__(self, **kwargs) -> None:
        self._items = types.MappingProxyType(kwargs)

    def __getitem__(self, key: str) -> typing.Any:
        return self._items[key]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._items)

    def __hash__(self):
        return hash(tuple(sorted(self.items())))


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
    def get_params(self) -> Params:
        """Get hyper-parameters of this actor.
        """

    @abc.abstractmethod
    def set_params(self, params: Params) -> None:
        """Set hyper-parameters of this actor.

        Args:
            params: Dictionary of hyper parameters.
        """

    @abc.abstractmethod
    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        Returns: state as bytes.
        """

    @abc.abstractmethod
    def set_state(self, state: bytes) -> None:
        """Set new internal state of the actor.

        Args:
            state: bytes to be used as internal state.
        """


class Spec(collections.namedtuple('Spec', 'actor, params')):
    """Wrapper of actor class and init params.
    """
