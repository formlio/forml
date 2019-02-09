from examples import *
from forml import flow

rfc = RFC(max_depth=3)

composer = flow.Composer(source, rfc)

render(composer)
