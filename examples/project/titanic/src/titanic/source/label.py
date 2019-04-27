import pandas
import typing

from forml.flow import task
from forml.stdlib.operator import simple


class LabelExtractor(task.Actor):
    """Custom label-extraction logic.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[pandas.DataFrame, pandas.Series]:
        return features.drop(columns=self._column), features[self._column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, column: str) -> None:
        self._column = column


EXTRACTOR = simple.Labeler(LabelExtractor.spec(column='Survived'))
