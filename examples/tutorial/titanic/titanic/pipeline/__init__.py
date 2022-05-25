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

This is one of the main _formal_ components that's being looked up by the ForML project loader.
"""
from sklearn import model_selection
from titanic.pipeline import preprocessing

from forml import project
from forml.pipeline import ensemble, wrap

# Let's import number of the sklearn classifiers:
# using the ``wrap.importer`` they'll transparently get converted into ForML operators
with wrap.importer():
    # pylint: disable=ungrouped-imports
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.naive_bayes import GaussianNB
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.svm import SVC
    from sklearn.tree import DecisionTreeClassifier


# Ensembling number of base models using the Stacking Generalization:
STACK = ensemble.FullStack(  # here FullStack is the particular ensembler implementation
    GradientBoostingClassifier(random_state=42),
    SVC(kernel='rbf', random_state=42, probability=True),
    SVC(kernel='linear', random_state=42, probability=True),
    KNeighborsClassifier(n_neighbors=5, metric='minkowski', p=2),
    GaussianNB(),
    RandomForestClassifier(n_estimators=10, criterion='entropy', random_state=42),
    DecisionTreeClassifier(criterion='entropy', random_state=42),
    # and selecting ``StratifiedKFold`` as the internal cross-validator for generating the stack folds
    crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42),
)


# And finally let's put together the actual pipeline composition using our preprocessing operators and the
# model ensemble:
PIPELINE = (
    preprocessing.impute(random_state=42)
    # >> payload.Dump(path='/tmp/tit/impute-$mode-$seq.csv')
    >> preprocessing.parse_title(source='Name', target='Title')  # pylint: disable=no-value-for-parameter
    >> preprocessing.encode(columns=['Sex', 'Embarked', 'Title', 'Pclass'])
    >> StandardScaler(copy=False)
    # >> payload.Dump(path='/tmp/tit/pretrain-$mode-$seq.csv')
    >> STACK
    # >> payload.Dump(path='/tmp/tit/stack-$mode-$seq.csv')
    >> LogisticRegression(random_state=42)
)

# Registering the pipeline
project.setup(PIPELINE)
