# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Folding ensembles unit tests.
"""
# pylint: disable=no-self-use
import pandas
from sklearn import model_selection

from forml import testing
from forml.pipeline import decorate, ensemble, payload

with decorate.autowrap():
    from sklearn.dummy import DummyClassifier  # pylint: disable=ungrouped-imports


class TestFullStack(testing.operator(ensemble.FullStack)):
    """FullStack operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1, 1, 1, 1, 1, 1]})
    LABELS = pandas.Series([0, 0, 1, 1, 1, 0], name='bar')
    CROSSVALIDATOR = model_selection.PredefinedSplit([0, 0, 0, 1, 1, 1])
    MODEL1 = DummyClassifier(strategy='most_frequent')
    MODEL2 = DummyClassifier(strategy='most_frequent')
    TRAIN_EXPECT = pandas.DataFrame({0: [1.0, 1.0, 1.0, 0.0, 0.0, 0.0], 1: [1.0, 1.0, 1.0, 0.0, 0.0, 0.0]})
    APPLY_EXPECT = pandas.DataFrame({0: [0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 1: [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]})

    missing_bases = testing.Case().raises(ValueError, 'Base models required')
    missing_cval = testing.Case(MODEL1, MODEL2).raises(TypeError, 'Invalid combination')
    extra_nsplits = testing.Case(MODEL1, MODEL2, crossvalidator='foo', nsplits=2).raises(
        TypeError, 'Invalid combination'
    )
    extra_nsplits_missing_cval = testing.Case(MODEL1, MODEL2, nsplits=2).raises(TypeError, 'Invalid combination')
    missing_nsplits = testing.Case(MODEL1, MODEL2, splitter=payload.PandasCVFolds.spec()).raises(
        TypeError, 'Invalid combination'
    )
    extra_cval = testing.Case(
        MODEL1, MODEL2, crossvalidator='foo', splitter=payload.PandasCVFolds.spec(), nsplits=2
    ).raises(TypeError, 'Invalid combination')
    extra_cval_missing_nsplits = testing.Case(
        MODEL1, MODEL2, crossvalidator='foo', splitter=payload.PandasCVFolds.spec()
    ).raises(TypeError, 'Invalid combination')
    few_splits = testing.Case(MODEL1, MODEL2, splitter=payload.PandasCVFolds.spec(), nsplits=1).raises(
        ValueError, 'splits required'
    )

    train_mode = (
        testing.Case(MODEL1, MODEL2, crossvalidator=CROSSVALIDATOR)
        .train(FEATURES, LABELS)
        .returns(TRAIN_EXPECT, testing.pandas_equals)
    )
    apply_mode = (
        testing.Case(MODEL1, MODEL2, crossvalidator=CROSSVALIDATOR)
        .train(FEATURES, LABELS)
        .apply(FEATURES)
        .returns(APPLY_EXPECT, testing.pandas_equals)
    )
