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

This is one of the main _formal_ forml components (along with `source` and `evaluation`) that's being looked up by
the forml loader.
"""

from sklearn import metrics

from forml import evaluation, project

# Typical method of providing component implementation using `component.setup()`. Choosing the CrossVal method
# to implement classic cross-validated metric scoring
EVAL = project.Evaluation(
    evaluation.Function(metrics.log_loss),
    # evaluation.CrossVal(crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42)),
    evaluation.HoldOut(test_size=0.2, stratify=True, random_state=42),
)
project.setup(EVAL)
