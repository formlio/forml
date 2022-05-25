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

import demos
import numpy as np
import pandas as pd

from forml.pipeline import wrap


@wrap.Actor.train
def impute_age(
    state: typing.Optional[dict[str, typing.Any]],  # pylint: disable=unused-argument
    X: pd.DataFrame,
    y: pd.Series,  # pylint: disable=unused-argument
    random_state: typing.Optional[int] = None,  # pylint: disable=unused-argument
) -> dict[str, typing.Any]:
    """Train part of a stateful transformer for missing age imputation."""
    return {'age_mean': X['Age'].mean(), 'age_std': X['Age'].std()}


@wrap.Mapper.operator
@impute_age.apply
def impute_age(
    state: dict[str, typing.Any], X: pd.DataFrame, random_state: typing.Optional[int] = None
) -> pd.DataFrame:
    """Apply part of a stateful transformer for missing age imputation."""
    na_slice = X['Age'].isna()
    if na_slice.any():
        rand_age = np.random.default_rng(random_state).integers(
            state['age_mean'] - state['age_std'], state['age_mean'] + state['age_std'], size=na_slice.sum()
        )
        X.loc[na_slice, 'Age'] = rand_age
    return X


PIPELINE = impute_age(random_state=42) >> demos.LR(
    max_iter=3, solver='lbfgs'
)  # pylint: disable=unexpected-keyword-arg, no-value-for-parameter

PROJECT = demos.SOURCE.bind(PIPELINE)

if __name__ == '__main__':
    PROJECT.launcher('graphviz', [demos.FEED]).train()
