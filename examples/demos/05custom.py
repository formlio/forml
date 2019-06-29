from demos import *
from forml.flow import task
from forml.stdlib.operator import simple


@simple.Mapper.operator
class NaNImputer(task.Actor):
    """Custom NaN imputation logic.
    """
    def train(self, features: pd.DataFrame, label: pd.DataFrame):
        """Impute missing values using the median for numeric columns and the most common value for string columns.
        """
        self._fill = pd.Series([features[f].value_counts().index[0] if features[f].dtype == np.dtype('O')
                                    else features[f].median() for f in features], index=features.columns)
        return self

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filling the NaNs.
        """
        return df.fillna(self.fill)

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Mandatory get params.
        """
        return {}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Mandatory set params.
        """
        pass


PIPELINE = NaNImputer() >> LR(max_iter=3, solver='lbfgs')

PROJECT = SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
