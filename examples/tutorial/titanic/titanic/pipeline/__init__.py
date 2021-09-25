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
Titanic project pipeline.

This is one of the main _formal_ forml components (along with `source` and `evaluation`) that's being looked up by
the forml loader. In this case it is implemented as a python package but it could be as well just a module
`pipeline.py`.

All the submodules of this packages have no semantic meaning for ForML - they are completely informal and have been
created just for structuring the project code base splitting it into these particular parts with arbitrary names.
"""
from sklearn import model_selection
from titanic.pipeline import preprocessing, model

from forml.lib.pipeline import ensemble
from forml.project import component

# Stack of models implemented based on the forml lib ensembler supplied with standard sklearn Random Forest and
# Gradient Boosting Classifiers using the sklearn StratifiedKFold crossvalidation splitter.
STACK = ensemble.FullStack(
    bases=(model.RFC(n_estimators=10, random_state=42), model.GBC(random_state=42)),
    crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42),
)


# This is the main pipeline composition:
FLOW = (
    preprocessing.NaNImputer()
    >> preprocessing.parse_title(source='Name', target='Title')
    >> preprocessing.ENCODER(cols=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked', 'Title'])
    >> STACK
    >> model.LR(random_state=42, solver='lbfgs')
)

# And the final step is registering the pipeline instance as the forml component:
component.setup(FLOW)
