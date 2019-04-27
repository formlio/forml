"""
Titanic project pipeline.
"""

from titanic.pipeline import preprocessing, model
from forml.project import component

INSTANCE = preprocessing.NANIMPUTER >> preprocessing.TITLEPARSER >> preprocessing.ENCODER >> model.STACK >> model.LR
component.setup(INSTANCE)
