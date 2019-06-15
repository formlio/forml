from demos import *

PIPELINE = SimpleImputer(strategy='mean') >> LR(max_iter=3, solver='lbfgs')

PROJECT = SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
