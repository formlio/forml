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
Transformers useful for the Titanic dataset pre-processing.
"""
# pylint: disable=invalid-name, unused-argument

import typing

import numpy
import pandas
from sklearn import preprocessing

from forml.pipeline import wrap

# There is a number of different ways ForML allows to implement actors/operators. Here we use the
# easiest approach of wrapping plain functions using the decorators from the ``forml.pipeline.wrap``
# package:


@wrap.Operator.mapper
@wrap.Actor.apply
def ParseTitle(
    features: pandas.DataFrame,
    *,
    source: str,
    target: str,
) -> pandas.DataFrame:
    """Transformer extracting a person's title from the name string."""

    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title."""
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip().lower()
        return 'n/a'

    features[target] = features[source].map(get_title)
    return features.drop(columns=source)


# sphinx: Impute start
@wrap.Actor.train
def Impute(
    state: typing.Optional[dict[str, float]],
    features: pandas.DataFrame,
    labels: pandas.Series,
    random_state: typing.Optional[int] = None,
) -> dict[str, float]:
    """Train part of a stateful transformer for missing values imputation."""
    return {
        'age_avg': features['Age'].mean(),
        'age_std': features['Age'].std(ddof=0),
        'fare_avg': features['Fare'].mean(),
    }


@wrap.Operator.mapper
@Impute.apply
def Impute(
    state: dict[str, float], features: pandas.DataFrame, random_state: typing.Optional[int] = None
) -> pandas.DataFrame:
    """Apply part of a stateful transformer for missing values imputation."""
    age_nan = features['Age'].isna()
    if age_nan.any():
        age_rnd = numpy.random.default_rng(random_state).integers(
            state['age_avg'] - state['age_std'], state['age_avg'] + state['age_std'], size=age_nan.sum()
        )
        features.loc[age_nan, 'Age'] = age_rnd  # random age with same distribution
    features.loc[:, 'Embarked'] = features['Embarked'].fillna('S')  # assuming Southampton
    features.loc[:, 'Fare'] = features['Fare'].fillna(state['fare_avg'])  # mean fare
    assert not features.isna().any().any(), 'NaN still'
    return features


# sphinx: Impute end


# sphinx: Encode start
@wrap.Actor.train
def Encode(
    state: typing.Optional[preprocessing.OneHotEncoder],
    features: pandas.DataFrame,
    labels: pandas.Series,
    columns: typing.Sequence[str],
) -> preprocessing.OneHotEncoder:
    """Train part of a stateful encoder for the various categorical features."""
    encoder = preprocessing.OneHotEncoder(handle_unknown='infrequent_if_exist', sparse_output=False)
    encoder.fit(features[columns])
    return encoder


@wrap.Operator.mapper
@Encode.apply
def Encode(
    state: preprocessing.OneHotEncoder, features: pandas.DataFrame, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """Apply part of a stateful encoder for the various categorical features."""
    onehot = pandas.DataFrame(state.transform(features[columns]))
    result = pandas.concat((features.drop(columns=columns), onehot), axis='columns')
    result.columns = [str(c) for c in result.columns]
    return result


# sphinx: Encode end
