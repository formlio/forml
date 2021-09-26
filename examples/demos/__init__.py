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

"""
Forml demos.
"""

import typing

import pandas as pd
from sklearn import ensemble, feature_extraction, impute, linear_model, naive_bayes, preprocessing

from forml import flow
from forml.io import dsl
from forml.lib.feed import static
from forml.lib.pipeline import payload, topology
from forml.project import component

SimpleImputer = topology.Mapper.operator(topology.Class.actor(impute.SimpleImputer, train='fit', apply='transform'))

OneHotEncoder = topology.Mapper.operator(
    topology.Class.actor(preprocessing.OneHotEncoder, train='fit', apply='transform')
)

Binarizer = topology.Mapper.operator(topology.Class.actor(preprocessing.Binarizer, train='fit', apply='transform'))

FeatureHasher = topology.Mapper.operator(
    topology.Class.actor(feature_extraction.FeatureHasher, train='fit', apply='transform')
)

RFC = topology.Consumer.operator(
    topology.Class.actor(ensemble.RandomForestClassifier, train='fit', apply='predict_proba')
)

GBC = topology.Consumer.operator(
    topology.Class.actor(ensemble.GradientBoostingClassifier, train='fit', apply='predict_proba')
)

LR = topology.Consumer.operator(
    topology.Class.actor(linear_model.LogisticRegression, train='fit', apply='predict_proba')
)

Bayes = topology.Consumer.operator(topology.Class.actor(naive_bayes.BernoulliNB, train='fit', apply='predict_proba'))


@topology.Labeler.operator
class Extractor(flow.Actor):
    """Here we just create a custom actor that simply expects the label to be a specific column in the input dataset and
    returns two objects - a dataframe without the label column and a series with just the labels.
    """

    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        return df.drop(columns=self._column), df[self._column]

    def get_params(self) -> dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, column: str) -> None:
        self._column = column


@topology.Mapper.operator
@topology.Function.actor
def cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Simple stateless transformer create from a plain function."""
    return df.dropna()


class Demo(dsl.Schema):
    """Demo schema representation."""

    Label = dsl.Field(dsl.Integer())
    Age = dsl.Field(dsl.Integer())


DATA = [[1, 1, 1, 0, 0, 0], [10, 11, 12, 13, 14, 15]]


class Feed(static.Feed):
    """Demo feed."""

    def __init__(self):
        super().__init__({Demo: DATA})


FEED = Feed()
SOURCE = component.Source.query(Demo.select(Demo.Age), Demo.Label) >> payload.to_pandas(columns=['Age'])
