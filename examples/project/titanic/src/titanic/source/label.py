"""
Label extraction logic.

This module is informal from ForML perspective and has been created just for structuring the project code base.
"""

import typing

import pandas

from forml.flow import task
from forml.stdlib.operator import simple


@simple.Labeler.operator
class Extractor(task.Actor):
    """Here we just create a custom actor that simply expects the label to be a specific column in the input dataset and
    returns two objects - a dataframe without the label column and a series with just the labels.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[pandas.DataFrame, pandas.Series]:
        return features.drop(columns=self._column), features[self._column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, column: str) -> None:
        self._column = column
