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
ForML demo 1 - Ensemble.
"""
# pylint: disable=ungrouped-imports
import demos
from sklearn import model_selection

from forml.pipeline import ensemble, wrap

with wrap.importer():
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression

STACK = ensemble.FullStack(
    RandomForestClassifier(max_depth=3),
    GradientBoostingClassifier(max_depth=3),
    crossvalidator=model_selection.StratifiedKFold(n_splits=2),
)

PIPELINE = SimpleImputer(strategy='mean') >> STACK >> LogisticRegression(max_iter=3, solver='lbfgs')

LAUNCHER = demos.SOURCE.bind(PIPELINE).launcher('visual', feeds=[demos.FEED])

if __name__ == '__main__':
    LAUNCHER.apply()
