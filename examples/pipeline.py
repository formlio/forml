import typing

import pandas
import numpy
from sklearn import ensemble as sklearn

from forml import flow
from forml.exec.runtime import visual
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


# Turning sklearn RFC into a pipeline operator
RFC = simple.Consumer.operator(task.Wrapped.actor(sklearn.RandomForestClassifier, train='fit', apply='predict_proba'))


pipeline = flow.Pipeline(LabelExtractor(column='foo') >> NaNImputer() >> RFC(max_depth=3))

# Collect both the train and apply graph dags
dag = visual.Dot('Pipeline', format='png')
pipeline.train.accept(dag)
pipeline.apply.accept(dag)
print(dag.source)
dag.render('/tmp/pipeline.gv')
