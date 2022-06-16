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
    def splitter_builder() -> flow.Builder[payload.CVFoldable]:
        """Splitter builder fixture."""
        return payload.PandasCVFolds.builder()

    def test_init(self, crossvalidator: payload.CrossValidable, splitter_builder: flow.Builder[payload.CVFoldable]):
        """Init tests."""
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal()  # missing crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, nsplits=2)  # extra nsplits
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(nsplits=2)  # extra nsplits, missing crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(splitter=splitter_builder)  # missing nsplits
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, splitter=splitter_builder, nsplits=2)  # extra cval
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.CrossVal(crossvalidator=crossvalidator, splitter=splitter_builder)  # extra cval, missing nsplits
        with pytest.raises(ValueError, match='splits required'):
            evaluation.CrossVal(splitter=splitter_builder, nsplits=1)  # few splits


class TestHoldOut:
    """HoldOut method unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def crossvalidator() -> payload.CrossValidable:
        """Crossvalidator fixture."""
        return model_selection.PredefinedSplit([0, 0, 0, 1, 1, 1])

    @staticmethod
    @pytest.fixture(scope='session')
    def splitter_builder() -> flow.Builder[payload.CVFoldable]:
        """Splitter builder fixture."""
        return payload.PandasCVFolds.builder()

    def test_init(self, crossvalidator: payload.CrossValidable, splitter_builder: flow.Builder[payload.CVFoldable]):
        """Init tests."""
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(test_size=0.2, crossvalidator=crossvalidator)  # extra test_size/crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(train_size=0.2, crossvalidator=crossvalidator)  # extra train_size/crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(random_state=42, crossvalidator=crossvalidator)  # extra random/crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(stratify=True, crossvalidator=crossvalidator)  # extra stratify/crossvalidator
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(test_size=0.2, splitter=splitter_builder)  # extra test_size/splitter builder
        with pytest.raises(TypeError, match='Invalid combination'):
            evaluation.HoldOut(crossvalidator=crossvalidator, splitter=splitter_builder)  # extra cval
