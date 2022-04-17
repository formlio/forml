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
Dummy project evaluation.
"""
from sklearn import metrics, model_selection

from forml import evaluation, project

INSTANCE = project.Evaluation(
    evaluation.Function(metrics.log_loss),
    evaluation.CrossVal(model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42)),
)
project.setup(INSTANCE)
