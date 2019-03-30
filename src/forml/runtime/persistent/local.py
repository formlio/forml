import typing

from forml import runtime
from forml.runtime import resource


class Registry(runtime.Registry):
    PATH = ...

    @classmethod
    def load(cls, name: typing.Optional[str] = None) -> resource.Parcel:
        pass

    @classmethod
    def save(cls, parcel: resource.Parcel) -> None:
        parcel.training.timestamp
