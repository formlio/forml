"""
Titanic project pipeline.

This is one of the main _formal_ forml components (along with `source` and `evaluation`) that's being looked up by
the forml loader. In this case it is implemented as a python package but it could be as well just a module
`pipeline.py`.

All the submodules of this packages have no semantic meaning for ForML - they are completely informal and have been
created just for structuring the project code base splitting it into these particular parts with arbitrary names.
"""

from sklearn import model_selection

from forml.project import component
from forml.stdlib.operator.folding import ensemble
from titanic.pipeline import preprocessing, model

# Stack of models implemented based on the forml stdlib ensembler supplied with standard sklearn Random Forest and
# Gradient Boosting Classifiers using the sklearn StratifiedKFold crossvalidation splitter.
STACK = ensemble.FullStacker(bases=(model.RFC(n_estimators=10, random_state=42),
                                    model.GBC(random_state=42)),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42))


# This is the main pipeline composition:
INSTANCE = preprocessing.NaNImputer() >> \
           preprocessing.parse_title(source='Name', target='Title') >> \
           preprocessing.ENCODER(cols=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked', 'Title']) >> \
           STACK >> \
           model.LR(random_state=42)

# And the final step is registering the pipeline instance as the forml component:
component.setup(INSTANCE)
