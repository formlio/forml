import typing

from forml import etl
from forml.etl import expression
from forml.flow import task


class Emitor(task.Actor):
    """Custom label-extraction logic.
    """
    def apply(self, features: typing.Any) -> typing.Any:
        return features

    def set_params(self, params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
                   **kwparams: typing.Any) -> None:
        return


class Engine(etl.Engine):
    def setup(self, select: expression.Select, lower: typing.Optional[etl.OrdinalT],
              upper: typing.Optional[etl.OrdinalT]) -> task.Spec:
        return task.Spec(Emitor)
