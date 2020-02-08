from forml import testing
from forml.testing import spec
from titanic.pipeline import preprocessing


class MyTest(testing.operator(preprocessing.NaNImputer)):
    invalid_params = spec.Case(foo=1).raises(ValueError)
    not_trained = spec.Case(foo=1).apply('foo').raises(ValueError)
