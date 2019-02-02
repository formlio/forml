import numpy
import pandas

from examples import *
from forml import flow
from forml.flow import task
from forml.flow.operator import simple


@simple.Labeler.operator
class LabelExtractor(task.Actor[pandas.DataFrame]):
    """Custom label-extraction logic.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:
        return features[0][[self._column]]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        self._column = params.get('column', self._column)


@simple.Mapper.operator
class NaNImputer(task.Actor[pandas.DataFrame]):
    """Custom NaN imputation logic.
    """
    def train(self, features: pandas.DataFrame, label: pandas.DataFrame):
        """Impute missing values using the median for numeric columns and the most common value for string columns.
        """
        self._fill = pandas.Series([features[f].value_counts().index[0] if features[f].dtype == numpy.dtype('O')
                                    else features[f].median() for f in features], index=features.columns)
        return self

    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:
        """Filling the NaNs.
        """
        return features.fillna(self.fill)

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Mandatory get params.
        """
        return {}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Mandatory set params.
        """
        pass


labelx = LabelExtractor(column='foo')
imputer = SimpleImputer(strategy='mean')
lr = LR(max_depth=3)

pipeline = flow.Pipeline(labelx >> imputer >> lr)

render(pipeline)
