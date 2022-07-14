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

from forml import project
from forml.io import dsl
from forml.pipeline import payload, wrap
from forml.provider.feed import swiss

with wrap.importer():  # automatically converting the particular SKLearn classes to ForML operators
    from sklearn.ensemble import GradientBoostingClassifier as GBC
    from sklearn.ensemble import RandomForestClassifier as RFC
    from sklearn.feature_extraction import FeatureHasher
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression as LR
    from sklearn.naive_bayes import BernoulliNB as Bayes
    from sklearn.preprocessing import Binarizer, OneHotEncoder


__all__ = ['GBC', 'RFC', 'FeatureHasher', 'SimpleImputer', 'LR', 'Bayes', 'Binarizer', 'OneHotEncoder']


@wrap.Operator.mapper
@wrap.Actor.apply
def cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """Simple stateless transformer create from a plain function."""
    return df.dropna()


class Demo(dsl.Schema):
    """Demo schema representation."""

    Label = dsl.Field(dsl.Integer())
    Age = dsl.Field(dsl.Integer())


DATA = [[1, 1, 1, 0, 0, 0], [10, 11, 12, 13, 14, 15]]


class Feed(swiss.Feed):
    """Demo feed."""

    def __init__(self):
        super().__init__({Demo: DATA})


FEED = Feed()
SOURCE = project.Source.query(Demo.select(Demo.Age), Demo.Label) >> payload.ToPandas(columns=['Age'])
