import demos

PIPELINE = demos.RFC(max_depth=3)

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
