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
ForML demo 1 - Custom.
"""
# pylint: disable=invalid-name, no-value-for-parameter, disable=unused-argument
import typing

import demos
import numpy as np
import pandas as pd

from forml.pipeline import wrap

with wrap.importer():
    from sklearn.linear_model import LogisticRegression


@wrap.Actor.train
def ImputeAge(
    state: typing.Optional[dict[str, typing.Any]],
    X: pd.DataFrame,
    y: pd.Series,
    random_state: typing.Optional[int] = None,
) -> dict[str, typing.Any]:
    """Train part of a stateful transformer for missing age imputation."""
    return {'age_mean': X['Age'].mean(), 'age_std': X['Age'].std()}


@wrap.Operator.mapper
@ImputeAge.apply
def ImputeAge(state: dict[str, typing.Any], X: pd.DataFrame, random_state: typing.Optional[int] = None) -> pd.DataFrame:
    """Apply part of a stateful transformer for missing age imputation."""
    na_slice = X['Age'].isna()
    if na_slice.any():
        rand_age = np.random.default_rng(random_state).integers(
            state['age_mean'] - state['age_std'], state['age_mean'] + state['age_std'], size=na_slice.sum()
        )
        X.loc[na_slice, 'Age'] = rand_age
    return X


PIPELINE = ImputeAge(random_state=42) >> LogisticRegression(max_iter=3, solver='lbfgs')

LAUNCHER = demos.SOURCE.bind(PIPELINE).launcher('visual', feeds=[demos.FEED])

if __name__ == '__main__':
    LAUNCHER.apply()
