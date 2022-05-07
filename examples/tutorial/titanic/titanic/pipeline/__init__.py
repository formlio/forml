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
from titanic.pipeline import preprocessing

from forml import project
from forml.pipeline import ensemble, wrap

with wrap.importer():
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.naive_bayes import GaussianNB
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.svm import SVC
    from sklearn.tree import DecisionTreeClassifier

# Stack of models implemented based on the forml lib ensembler supplied with standard sklearn Random Forest and
# Gradient Boosting Classifiers using the sklearn StratifiedKFold crossvalidation splitter.
STACK = ensemble.FullStack(
    GradientBoostingClassifier(random_state=42),
    SVC(kernel='rbf', random_state=42, probability=True),
    SVC(kernel='linear', random_state=42, probability=True),
    KNeighborsClassifier(n_neighbors=5, metric='minkowski', p=2),
    GaussianNB(),
    RandomForestClassifier(n_estimators=10, criterion='entropy', random_state=42),
    DecisionTreeClassifier(criterion='entropy', random_state=42),
    crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42),
)


# This is the main pipeline composition:
FLOW = (
    preprocessing.impute(random_state=42)
    # >> payload.Dump(path='/tmp/tit/impute-$mode-$seq.csv')
    >> preprocessing.parse_title(source='Name', target='Title')
    >> preprocessing.encode(columns=['Sex', 'Embarked', 'Title', 'Pclass'])
    >> StandardScaler(copy=False)
    # >> payload.Dump(path='/tmp/tit/pretrain-$mode-$seq.csv')
    >> STACK
    # >> payload.Dump(path='/tmp/tit/stack-$mode-$seq.csv')
    >> LogisticRegression(random_state=42)
)

# And the final step is registering the pipeline instance as the forml component:
project.setup(FLOW)
