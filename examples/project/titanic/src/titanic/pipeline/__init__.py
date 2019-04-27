"""
Titanic project pipeline.
"""

from sklearn import model_selection
from titanic.pipeline import preprocessing, model

from forml.project import component
from forml.stdlib.operator import ensemble

STACK = ensemble.Stack(bases=(model.RFC(n_estimators=10, random_state=42),
                              model.GBC(random_state=42)),
                       crossvalidator=model_selection.KFold(n_splits=3, shuffle=True, random_state=42))


INSTANCE = preprocessing.NANIMPUTER() >> \
           preprocessing.TITLEPARSER(source='Name', target='Title') >> \
           preprocessing.ENCODER(cols=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked', 'Title']) >> \
           STACK >> \
           model.LR(random_state=42)
component.setup(INSTANCE)
