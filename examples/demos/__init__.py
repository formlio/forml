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


import pandas as pd
from sklearn import ensemble, feature_extraction, impute, linear_model, naive_bayes, preprocessing

from forml import project
from forml.io import dsl
from forml.pipeline import payload, wrap
from forml.provider.feed import static

SimpleImputer = wrap.Mapper.operator(wrap.Actor.type(impute.SimpleImputer, train='fit', apply='transform'))

OneHotEncoder = wrap.Mapper.operator(wrap.Actor.type(preprocessing.OneHotEncoder, train='fit', apply='transform'))

Binarizer = wrap.Mapper.operator(wrap.Actor.type(preprocessing.Binarizer, train='fit', apply='transform'))

FeatureHasher = wrap.Mapper.operator(wrap.Actor.type(feature_extraction.FeatureHasher, train='fit', apply='transform'))

RFC = wrap.Consumer.operator(wrap.Actor.type(ensemble.RandomForestClassifier, train='fit', apply='predict_proba'))

GBC = wrap.Consumer.operator(wrap.Actor.type(ensemble.GradientBoostingClassifier, train='fit', apply='predict_proba'))

LR = wrap.Consumer.operator(wrap.Actor.type(linear_model.LogisticRegression, train='fit', apply='predict_proba'))

Bayes = wrap.Consumer.operator(wrap.Actor.type(naive_bayes.BernoulliNB, train='fit', apply='predict_proba'))


@wrap.Mapper.operator
@wrap.Actor.type
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
SOURCE = project.Source.query(Demo.select(Demo.Age), Demo.Label) >> payload.ToPandas(columns=['Age'])
