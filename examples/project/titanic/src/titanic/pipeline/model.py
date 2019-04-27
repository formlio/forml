from sklearn import ensemble as estimator
from sklearn import linear_model

from forml.stdlib import actor
from forml.stdlib.operator import simple

RFC = simple.Consumer.operator(actor.Wrapped.actor(
    estimator.RandomForestClassifier, train='fit', apply='predict_proba'))
GBC = simple.Consumer.operator(actor.Wrapped.actor(
    estimator.GradientBoostingClassifier, train='fit', apply='predict_proba'))

LR = simple.Consumer.operator(actor.Wrapped.actor(linear_model.LogisticRegression, train='fit', apply='predict'))
