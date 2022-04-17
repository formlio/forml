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

import demos
from sklearn import model_selection

from forml.pipeline import ensemble

STACK = ensemble.FullStack(
    bases=(demos.RFC(max_depth=3), demos.GBC(max_depth=3)), crossvalidator=model_selection.StratifiedKFold(n_splits=2)
)

PIPELINE = demos.SimpleImputer(strategy='mean') >> STACK >> demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

if __name__ == '__main__':
    PROJECT.launcher('graphviz', [demos.FEED]).train()
