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
Evaluation methods unit tests.
"""
# pylint: disable=no-self-use
import pytest
from sklearn import model_selection

from forml import evaluation, flow
from forml.pipeline import payload


class TestCrossVal:
    """CrossVal method unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def crossvalidator() -> payload.CrossValidable:
        """Crossvalidator fixture."""
        return model_selection.PredefinedSplit([0, 0, 0, 1, 1, 1])

    @staticmethod
    @pytest.fixture(scope='session')
    def splitter_spec() -> flow.Spec[payload.CVFoldable]:
        """Splitter spec fixture."""
        return payload.PandasCVFolds.spec()

    def test_init(self, crossvalidator: payload.CrossValidable, splitter_spec: flow.Spec[payload.CVFoldable]):
        """Init tests."""
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal()  # missing crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, nsplits=2)  # extra nsplits
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(nsplits=2)  # extra nsplits, missing crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(splitter=splitter_spec)  # missing nsplits
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, splitter=splitter_spec, nsplits=2)  # extra cval
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, splitter=splitter_spec)  # extra cval, missing nsplits
        with pytest.raises(ValueError, match='splits required'):
            evaluation.CrossVal(splitter=splitter_spec, nsplits=1)  # few splits
