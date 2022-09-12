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
def ImputeNumeric(
    state: typing.Optional[dict[str, typing.Any]],
    X: pd.DataFrame,
    y: pd.Series,
    column: str,
    random_state: typing.Optional[int] = None,
) -> dict[str, typing.Any]:
    """Train part of a stateful transformer for missing numeric column values imputation."""
    return {'mean': X[column].mean(), 'std': X[column].std()}


@wrap.Operator.mapper
@ImputeNumeric.apply
def ImputeNumeric(
    state: dict[str, typing.Any], X: pd.DataFrame, column: str, random_state: typing.Optional[int] = None
) -> pd.DataFrame:
    """Apply part of a stateful transformer for missing numeric column values imputation."""
    na_slice = X[column].isna()
    if na_slice.any():
        rand_age = np.random.default_rng(random_state).integers(
            state['mean'] - state['std'], state['mean'] + state['std'], size=na_slice.sum()
        )
        X.loc[na_slice, column] = rand_age
    return X


PIPELINE = ImputeNumeric(column='Feature', random_state=42) >> LogisticRegression(max_iter=50, solver='lbfgs')

LAUNCHER = demos.SOURCE.bind(PIPELINE).launcher('visual', feeds=[demos.FEED])

if __name__ == '__main__':
    LAUNCHER.train(3, 6)  # train on the records with the Ordinal between 3 and 6
    # print(LAUNCHER.apply(7))  # predict for records with sequence ID 7 and above
