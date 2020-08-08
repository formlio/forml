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

from sklearn import model_selection

import demos
from forml.stdlib.operator.folding import ensemble

FH_RFC = demos.FeatureHasher(n_features=128) >> demos.RFC(n_estimators=20, n_jobs=4, max_depth=3)
BIN_BAYES = demos.Binarizer(threshold=0.63) >> demos.Bayes(alpha=1.1)

STACK = ensemble.FullStacker(bases=(FH_RFC, BIN_BAYES),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2))

PIPELINE = demos.SimpleImputer(strategy='mean') >> \
    demos.OneHotEncoder() >> \
    STACK >> \
    demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
