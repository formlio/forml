import typing

import pandas
from sklearn import ensemble, linear_model, impute, preprocessing, feature_extraction, naive_bayes

from forml import flow
from forml.exec.runtime import visual
from forml.flow import task
from forml.flow.operator import simple

SimpleImputer = simple.Mapper.operator(task.Wrapped.actor(
    impute.SimpleImputer, train='fit', apply='transform'))

OneHotEncoder = simple.Mapper.operator(task.Wrapped.actor(
    preprocessing.OneHotEncoder, train='fit', apply='transform'))

Binarizer = simple.Mapper.operator(task.Wrapped.actor(
    preprocessing.Binarizer, train='fit', apply='transform'))

FeatureHasher = simple.Mapper.operator(task.Wrapped.actor(
    feature_extraction.FeatureHasher, train='fit', apply='transform'))

RFC = simple.Consumer.operator(task.Wrapped.actor(
    ensemble.RandomForestClassifier, train='fit', apply='predict_proba'))

GBC = simple.Consumer.operator(task.Wrapped.actor(
    ensemble.GradientBoostingClassifier, train='fit', apply='predict_proba'))

LR = simple.Consumer.operator(task.Wrapped.actor(
    linear_model.LogisticRegression, train='fit', apply='predict_proba'))

Bayes = simple.Consumer.operator(task.Wrapped.actor(
    naive_bayes.BernoulliNB, train='fit', apply='predict_proba'))


@simple.Labeler.operator
class LabelExtractor(task.Actor[pandas.DataFrame]):
    """Custom label-extraction logic.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[pandas.DataFrame, pandas.DataFrame]:
        return features[[self._column]], features.drop(self._column, axis='columns')

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        self._column = params.get('column', self._column)


def render(pipeline: flow.Pipeline):
    dag = visual.Dot('Pipeline', format='png')
    pipeline.train.accept(dag)
    pipeline.apply.accept(dag)
    print(dag.source)
    dag.render('/tmp/pipeline.gv')
