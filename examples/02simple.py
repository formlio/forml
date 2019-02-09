from examples import *
from forml import flow


imputer = SimpleImputer(strategy='mean')
lr = LR(max_depth=3)

composer = flow.Composer(source, imputer >> lr)

render(composer)
