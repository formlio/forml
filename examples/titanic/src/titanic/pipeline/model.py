from forml.stdlib import actor
from forml.stdlib.operator import simple, ensemble
from sklearn import ensemble as estimator
from sklearn import linear_model
from sklearn import model_selection


RFC = simple.Consumer(actor.Wrapped.actor(
    estimator.RandomForestClassifier, train='fit', apply='predict_proba').spec(n_estimators=10, random_state=42))
GBC = simple.Consumer(actor.Wrapped.actor(
    estimator.GradientBoostingClassifier, train='fit', apply='predict_proba').spec(random_state=42))

LR = simple.Consumer(actor.Wrapped.actor(
    linear_model.LogisticRegression, train='fit', apply='predict').spec(random_state=42))

STACK = ensemble.Stack(bases=(RFC, GBC),
                       crossvalidator=model_selection.KFold(n_splits=3, shuffle=True, random_state=42))
