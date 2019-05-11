"""
Label extraction actors.
"""

import typing

import pandas

from forml.flow import task


class ColumnExtractor(task.Actor):
    """Column based label-extraction actor with 1:2 shape.
    """
    def __init__(self, column: str = 'label'):
        self.column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[  # pylint: disable=arguments-differ
            pandas.DataFrame, pandas.Series]:
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input features set.

        Returns: Features with label column removed plus just the label column in second new dataset.
        """
        return features.drop(columns=self.column), features[self.column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {'column': self.column}

    def set_params(self, column: str) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            column: Label column name.
        """
        self.column = column
