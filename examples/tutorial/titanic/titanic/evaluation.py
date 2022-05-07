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
Titanic evaluation definition.

This is one of the main _formal_ components that's being looked up by the ForML project loader.
"""
import numpy
from sklearn import metrics

from forml import evaluation, project

# Setting up the evaluation descriptor needs the following input:
# 1) Evaluation metric for the actual assessment of the prediction error
# 2) Evaluation method for out-of-sample evaluation (backtesting) - hold-out or cross-validation
EVALUATION = project.Evaluation(
    evaluation.Function(
        lambda t, p: metrics.accuracy_score(t, numpy.round(p))  # using accuracy as the metric for our project
    ),
    # alternatively we could simply switch to logloss:
    # evaluation.Function(metrics.log_loss),
    evaluation.HoldOut(test_size=0.2, stratify=True, random_state=42),  # hold-out as the backtesting method
    # alternatively we could switch to the cross-validation method instead of hold-out:
    # evaluation.CrossVal(crossvalidator=model_selection.StratifiedKFold(n_splits=3, shuffle=True, random_state=42)),
)

# Registering the descriptor
project.setup(EVALUATION)
