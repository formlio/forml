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
Evaluation stages unit tests.
"""
from sklearn import metrics, model_selection

from forml import evaluation, testing

METRIC = evaluation.Function(metrics.accuracy_score)
YTRUE = [0, 1, 2, 3]
YPRED = [0, 2, 1, 3]


class TestPerfTrackScore(testing.operator(evaluation.PerfTrackScore)):
    """Production evaluation stage operator unit test."""

    score = testing.Case(METRIC).train(YPRED, YTRUE).returns(0.5)


class TestTrainTestScore(testing.operator(evaluation.TrainTestScore)):
    """Development evaluation stage operator unit test."""

    METHOD = evaluation.CrossVal(crossvalidator=model_selection.PredefinedSplit([0, 0, 1, 1]))

    score = testing.Case(METRIC, METHOD).train(YPRED, YTRUE).returns(0.5)
