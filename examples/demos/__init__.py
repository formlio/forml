# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import typing

import pandas as pd
from forml.lib.flow.operator import cast

from forml.io.dsl import struct
from forml.io.dsl.struct import kind
from forml.lib.feed import static

from forml.project import component
from sklearn import ensemble, linear_model, impute, preprocessing, feature_extraction, naive_bayes

from forml.flow import task
from forml.lib.flow.actor import wrapped
from forml.lib.flow.operator.generic import simple

SimpleImputer = simple.Mapper.operator(wrapped.Class.actor(impute.SimpleImputer, train='fit', apply='transform'))

OneHotEncoder = simple.Mapper.operator(wrapped.Class.actor(preprocessing.OneHotEncoder, train='fit', apply='transform'))

Binarizer = simple.Mapper.operator(wrapped.Class.actor(preprocessing.Binarizer, train='fit', apply='transform'))

FeatureHasher = simple.Mapper.operator(
    wrapped.Class.actor(feature_extraction.FeatureHasher, train='fit', apply='transform')
)

RFC = simple.Consumer.operator(wrapped.Class.actor(ensemble.RandomForestClassifier, train='fit', apply='predict_proba'))

GBC = simple.Consumer.operator(
    wrapped.Class.actor(ensemble.GradientBoostingClassifier, train='fit', apply='predict_proba')
)

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
    """Simple stateless transformer create from a plain function."""
    return df.dropna()


class Demo(struct.Schema):
    """Demo schema representation."""

    Label = struct.Field(kind.Integer())
    Age = struct.Field(kind.Integer())


DATA = [[1, 1, 1, 0, 0, 0], [10, 11, 12, 13, 14, 15]]


class Feed(static.Feed):
    """Demo feed."""

    def __init__(self):
        super().__init__({Demo: DATA})


FEED = Feed()
SOURCE = component.Source.query(Demo.select(Demo.Age), Demo.Label) >> cast.ndframe(columns=['Age'])
