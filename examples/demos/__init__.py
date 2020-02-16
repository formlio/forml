import typing

import pandas as pd
from sklearn import ensemble, linear_model, impute, preprocessing, feature_extraction, naive_bayes

from forml import etl
from forml.etl import expression
from forml.flow import task
from forml.stdlib.actor import wrapped
from forml.stdlib.operator import simple

SimpleImputer = simple.Mapper.operator(wrapped.Class.actor(impute.SimpleImputer, train='fit', apply='transform'))

OneHotEncoder = simple.Mapper.operator(wrapped.Class.actor(preprocessing.OneHotEncoder, train='fit', apply='transform'))

Binarizer = simple.Mapper.operator(wrapped.Class.actor(preprocessing.Binarizer, train='fit', apply='transform'))

FeatureHasher = simple.Mapper.operator(wrapped.Class.actor(
    feature_extraction.FeatureHasher, train='fit', apply='transform'))

RFC = simple.Consumer.operator(wrapped.Class.actor(ensemble.RandomForestClassifier, train='fit', apply='predict_proba'))

GBC = simple.Consumer.operator(wrapped.Class.actor(
    ensemble.GradientBoostingClassifier, train='fit', apply='predict_proba'))

LR = simple.Consumer.operator(wrapped.Class.actor(linear_model.LogisticRegression, train='fit', apply='predict_proba'))

Bayes = simple.Consumer.operator(wrapped.Class.actor(naive_bayes.BernoulliNB, train='fit', apply='predict_proba'))


@simple.Labeler.operator
class Extractor(task.Actor):
    """Here we just create a custom actor that simply expects the label to be a specific column in the input dataset and
    returns two objects - a dataframe without the label column and a series with just the labels.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, pd.Series]:
        return df.drop(columns=self._column), df[self._column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, column: str) -> None:
        self._column = column


@simple.Mapper.operator
@wrapped.Function.actor
def cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Simple stateless transformer create from a plain function.
    """
    return df.dropna()


def trainset(**_) -> pd.DataFrame:
    return pd.DataFrame({'Survived': [1, 1, 1, 0, 0, 0], 'Age': [10, 11, 12, 13, 14, 15]})


def testset(**_) -> pd.DataFrame:
    return pd.DataFrame({'Age': [10, 11, 12, 13, 14, 15]})


TRAIN = expression.Select(trainset)
TEST = expression.Select(trainset)
SOURCE = etl.Extract(TRAIN, TEST) >> cleaner() >> Extractor(column='Survived')
