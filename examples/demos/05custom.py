
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

import numpy as np
import pandas as pd

import demos
from forml.flow import task
from forml.stdlib.operator import simple


@simple.Mapper.operator
class NaNImputer(task.Actor):
    """Custom NaN imputation logic.
    """
    def train(self, features: pd.DataFrame, label: pd.DataFrame):
        """Impute missing values using the median for numeric columns and the most common value for string columns.
        """
        self._fill = pd.Series([features[f].value_counts().index[0] if features[f].dtype == np.dtype('O')
                                else features[f].median() for f in features], index=features.columns)
        return self

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filling the NaNs.
        """
        return df.fillna(self.fill)

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Mandatory get params.
        """
        return {}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Mandatory set params.
        """
        pass


PIPELINE = NaNImputer() >> demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
