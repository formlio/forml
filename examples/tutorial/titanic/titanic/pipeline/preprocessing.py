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
Transformers useful for the Titanic example.

This module is informal from ForML perspective and has been created just for structuring the project code base.

Here we just create couple of forml operators that implement particular transformers.

We demonstrate three different ways of creating a forml operator:
  * Implementing native ForML actor (NanImputer)
  * Creating a wrapped actor from a plain function (parse_title)
  * Wrapping a 3rd party Transformer-like class (ENCODER)
"""
import typing

import numpy
import pandas
import pandas as pd
from sklearn import preprocessing

from forml.pipeline import wrap


@wrap.Mapper.operator
@wrap.Actor.apply
def parse_title(df: pd.DataFrame, *, source: str, target: str) -> pd.DataFrame:
    """Transformer extracting a person's title from the name string implemented as wrapped stateless function."""

    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title."""
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip().lower()
        return 'N/A'

    df[target] = df[source].map(get_title)
    return df.drop(columns=source)


@wrap.Actor.train
def impute(
    state: typing.Optional[dict[str, typing.Any]],
    features: pandas.DataFrame,
    labels: pandas.Series,
    random_state: typing.Optional[int] = None,
) -> dict[str, typing.Any]:
    """Missing values imputation."""
    return {'age_mean': features['Age'].mean(), 'age_std': features['Age'].std()}


@wrap.Mapper.operator
@impute.apply
def impute(
    state: dict[str, typing.Any], features: pandas.DataFrame, random_state: typing.Optional[int] = None
) -> pandas.DataFrame:
    na_slice = features['Age'].isna()
    if na_slice.any():
        rand_age = numpy.random.default_rng(random_state).integers(
            state['age_mean'] - state['age_std'], state['age_mean'] + state['age_std'], size=na_slice.sum()
        )
        features.loc[na_slice, 'Age'] = rand_age
    features['Embarked'].fillna('S', inplace=True)
    features['Fare'].fillna(features['Fare'].mean(), inplace=True)
    assert not features.isna().any().any(), 'NaN still'
    return features


@wrap.Actor.train
def encode(
    state: typing.Optional[preprocessing.OneHotEncoder],
    features: pandas.DataFrame,
    labels: pandas.Series,
    columns: typing.Sequence[str],
) -> preprocessing.OneHotEncoder:
    encoder = preprocessing.OneHotEncoder(handle_unknown='infrequent_if_exist', sparse=False)
    encoder.fit(features[columns])
    return encoder


@wrap.Mapper.operator
@encode.apply
def encode(
    state: preprocessing.OneHotEncoder, features: pandas.DataFrame, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    onehot = pandas.DataFrame(state.transform(features[columns]))
    return pandas.concat(
        (features.drop(columns=columns), onehot),
        axis='columns',
    )
