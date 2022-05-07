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
Pipeline models.

This module is informal from ForML perspective and has been created just for structuring the project code base.

Here we just create couple of forml operators that implement particular classifier.
"""

from sklearn import ensemble as estimator
from sklearn import linear_model

from forml.pipeline import wrap


def predict_proba(classifier, *args, **kwargs):
    """Apply decorator that calls predict_proba on the actor, transposes the output and returns the last columns
    (one-class probability).
    """
    return classifier.predict_proba(*args, **kwargs).transpose()[-1]


# Defining a forml operator by wrapping the standard sklearn classifier
RFC = wrap.Consumer.operator(wrap.Actor.type(estimator.RandomForestClassifier, train='fit', apply=predict_proba))

# Defining a forml operator by wrapping the standard sklearn classifier
GBC = wrap.Consumer.operator(
    wrap.Actor.type(
        estimator.GradientBoostingClassifier,
        train='fit',
        apply=predict_proba,
    )
)

# Defining a forml operator by wrapping the standard sklearn classifier
LR = wrap.Consumer.operator(wrap.Actor.type(linear_model.LogisticRegression, train='fit', apply=predict_proba))
