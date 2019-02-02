from examples import *
from forml import flow


labelx = LabelExtractor(column='foo')
imputer = SimpleImputer(strategy='mean')
lr = LR(max_depth=3)

pipeline = flow.Pipeline(labelx >> imputer >> lr)

render(pipeline)
