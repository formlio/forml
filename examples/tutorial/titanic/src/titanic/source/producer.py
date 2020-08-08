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
Data producer logic.

This module is informal from ForML perspective and has been created just for structuring the project code base.

Note ForML ETL implementation is currently a total stub so this is just a temporal way of feeding data into the system
and the eventual concept will be provided later.
"""

import os

import pandas as pd


BASE_DIR = os.path.dirname(__file__)
TRAINSET_CSV = os.path.join(BASE_DIR, 'titanic_train.csv')
TESTSET_CSV = os.path.join(BASE_DIR, 'titanic_test.csv')


def trainset(**_) -> pd.DataFrame:
    """Dummy trinset producer.
    """
    return pd.read_csv(TRAINSET_CSV)


def testset(**_) -> pd.DataFrame:
    """Dummy testset producer.
    """
    return pd.read_csv(TESTSET_CSV)
