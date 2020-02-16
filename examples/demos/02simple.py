import demos

PIPELINE = demos.SimpleImputer(strategy='mean') >> demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
