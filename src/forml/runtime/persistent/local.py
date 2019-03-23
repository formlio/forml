import typing

from forml import runtime
from forml.runtime import meta


class Registry(runtime.Registry):
    PATH = ...

    @classmethod
    def load(cls, name: typing.Optional[str] = None) -> meta.Parcel:
        pass

    @classmethod
    def save(cls, parcel: meta.Parcel) -> None:
        parcel.training.timestamp
