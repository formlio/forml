"""
Label extraction actors.
"""

import typing

import pandas

from forml.flow import task


class ColumnExtractor(task.Actor[pandas.DataFrame]):
    """Column based label-extraction actor with 1:2 shape.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame]:
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input features set.

        Returns: Features with label column removed plus just the label column in second new dataset.
        """
        return features.drop(columns=self._column), features[[self._column]]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {'column': self._column}

    def set_params(self, **params: typing.Any) -> None:
        """Standard params setter.

        Args:
            **params: New params.
        """
        self._column = params.get('column', self._column)
