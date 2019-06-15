from demos import *

PIPELINE = RFC(max_depth=3)

PROJECT = SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
