import numpy

from examples import *
from forml import flow
from forml.flow import task
from forml.flow.operator import simple


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


imputer = NaNImputer()
lr = LR(max_depth=3)

composer = flow.Composer(source, imputer >> lr)

render(composer)
