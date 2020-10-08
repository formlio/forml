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

import category_encoders
import numpy as np
import pandas as pd

from forml.flow import task
from forml.lib.actor import wrapped
from forml.lib.operator import simple


@simple.Mapper.operator
class NaNImputer(task.Actor):
    """Imputer for missing values implemented as native ForML actor.
    """
    def __init__(self):
        self._fill: typing.Optional[pd.Series] = None

    def train(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Train the actor by learning the median for each numeric column and finding the most common value for strings.
        """
        self._fill = pd.Series([X[c].value_counts().index[0] if X[c].dtype == np.dtype('O')
                                else X[c].median() for c in X], index=X.columns)

    def apply(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the imputation to the given dataset.
        """
        return X.fillna(self._fill)


@simple.Mapper.operator
@wrapped.Function.actor
def parse_title(df: pd.DataFrame, source: str, target: str) -> pd.DataFrame:
    """Transformer extracting a person's title from the name string implemented as wrapped stateless function.
    """
    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title.
        """
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip()
        return 'Unknown'

    df[target] = df[source].map(get_title)
    return df


# 3rd party transformer wrapped as an actor into a mapper operator:
ENCODER = simple.Mapper.operator(wrapped.Class.actor(category_encoders.HashingEncoder, train='fit', apply='transform'))
