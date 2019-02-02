from examples import *
from forml import flow


labelx = LabelExtractor(column='foo')
rfc = RFC(max_depth=3)

pipeline = flow.Pipeline(labelx >> rfc)

render(pipeline)
