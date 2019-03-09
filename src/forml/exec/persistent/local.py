import typing

from forml import exec
from forml.exec import meta


class Registry(exec.Registry):
    PATH = ...

    @classmethod
    def load(cls, name: typing.Optional[str] = None) -> meta.Parcel:
        pass

    @classmethod
    def save(cls, parcel: meta.Parcel) -> None:
        parcel.training.timestamp
