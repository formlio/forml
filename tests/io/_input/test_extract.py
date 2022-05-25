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
Extract utils unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml import project
from forml.io import dsl, layout
from forml.io._input import extract


class TestSlicer:
    """Slicer unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def features(project_components: project.Components) -> typing.Sequence[dsl.Feature]:
        """Features components fixture."""
        return project_components.source.extract.train.features

    @staticmethod
    @pytest.fixture(scope='session')
    def labels(project_components: project.Components) -> dsl.Feature:
        """Labels components fixture."""
        value = project_components.source.extract.labels
        assert isinstance(value, dsl.Feature)
        return value

    @staticmethod
    @pytest.fixture(scope='session')
    def dataset(trainset: layout.RowMajor) -> layout.Tabular:
        """Dataset fixture."""
        columns = layout.Dense.from_rows(trainset).to_columns()
        return layout.Dense.from_columns([*columns, columns[-1]])  # duplicating the last column

    @pytest.mark.parametrize(
        'label_factory, label_width',
        [
            (lambda l: l, 1),
            (lambda l: [l], 1),
            (lambda l: [l, l], 2),
        ],
    )
    def test_slicer(
        self,
        label_factory: typing.Callable[[dsl.Feature], typing.Union[dsl.Feature, typing.Sequence[dsl.Feature]]],
        label_width: int,
        features: typing.Sequence[dsl.Feature],
        labels: dsl.Feature,
        dataset: layout.Tabular,
    ):
        """Slicing test."""
        labels_fields = label_factory(labels)
        all_fields, slicer = extract.Slicer.from_columns(features, labels_fields)
        assert len(all_fields) == len(features) + label_width
        assert len(slicer.args[0]) == len(features)
        left, right = slicer().apply(dataset)
        columns = dataset.to_columns()
        assert len(left) == len(right) == len(columns[0])
        assert len(left[0]) == len(features)
        if isinstance(labels_fields, dsl.Feature):
            assert right[0] == columns[-1][0]
        else:
            assert len(right[0]) == label_width
