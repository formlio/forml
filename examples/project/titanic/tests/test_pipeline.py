from forml import testing
from titanic.pipeline import preprocessing
#
#
# with testing.Actor(preprocessing.NaNImputer.spec(1, 10)) as actor:
#     actor(*in_).raises(exception)
#     actor(*in_).returns(*out_)
#
#


# testing operator
# * separate test for apply, train, label

with testing.Operator(preprocessing.NaNImputer(1, 2, 3)) as operator:
    with operator(apply, train, label) as undertest:
        undertest.apply.equals(asd)
        undertest.train.raises(dsa)
        undertest.label == qwe

    operator.apply(*in_).returns(*out_)
    operator.train(*in_).raises(exception)
    operator.label(*in_).returns(None)


class TestNanImputer(testing.Operator(preprocessing.NaNImputer(1, 2, 3))):

    @testing.input(apply, train, label)
    def test_invalid(self):
        self.operator.apply(*in_).returns(*out_)
        operator.train(*in_).raises(exception)
        operator.label(*in_).returns(None)



hyperparams
train_in, label_in, apply_in -> train_out, label_out, apply_out
