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
import typing

import numpy
import pandas
from sklearn import preprocessing

from forml.pipeline import wrap

# There is a number of different ways ForML allows to implement actors/operators. Here we use the simplest approach
# of wrapping plain functions using the decorators from the ``forml.pipeline.wrap`` package:


@wrap.Mapper.operator
@wrap.Actor.apply
def parse_title(features: pandas.DataFrame, *, source: str, target: str) -> pandas.DataFrame:
    """Transformer extracting a person's title from the name string."""

    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title."""
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip().lower()
        return 'n/a'

    features[target] = features[source].map(get_title)
    return features.drop(columns=source)


@wrap.Actor.train
def impute(
    state: typing.Optional[dict[str, typing.Any]],  # pylint: disable=unused-argument
    features: pandas.DataFrame,
    labels: pandas.Series,  # pylint: disable=unused-argument
    random_state: typing.Optional[int] = None,  # pylint: disable=unused-argument
) -> dict[str, typing.Any]:
    """Train part of a stateful transformer for missing values imputation."""
    return {'age_mean': features['Age'].mean(), 'age_std': features['Age'].std()}


@wrap.Mapper.operator
@impute.apply
def impute(
    state: dict[str, typing.Any], features: pandas.DataFrame, random_state: typing.Optional[int] = None
) -> pandas.DataFrame:
    """Apply part of a stateful transformer for missing values imputation."""
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
    state: typing.Optional[preprocessing.OneHotEncoder],  # pylint: disable=unused-argument
    features: pandas.DataFrame,
    labels: pandas.Series,  # pylint: disable=unused-argument
    columns: typing.Sequence[str],
) -> preprocessing.OneHotEncoder:
    """Train part of a stateful encoder for the various categorical features."""
    encoder = preprocessing.OneHotEncoder(handle_unknown='infrequent_if_exist', sparse=False)
    encoder.fit(features[columns])
    return encoder


@wrap.Mapper.operator
@encode.apply
def encode(
    state: preprocessing.OneHotEncoder, features: pandas.DataFrame, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """Apply part of a stateful encoder for the various categorical features."""
    onehot = pandas.DataFrame(state.transform(features[columns]))
    return pandas.concat(
        (features.drop(columns=columns), onehot),
        axis='columns',
    )
