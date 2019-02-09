from examples import *
from forml import flow
from forml.flow.operator import ensemble


imputer = SimpleImputer(strategy='mean')
rfc = RFC(max_depth=3)
gbc = GBC(max_depth=3)
lr = LR(max_depth=3)
stack = ensemble.Stack(bases=(gbc, rfc), folds=2)

composer = flow.Composer(source, imputer >> stack >> lr)

render(composer)
