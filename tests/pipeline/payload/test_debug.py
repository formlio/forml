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
Debug payload operators unit tests.
"""
# pylint: disable=no-self-use
import pathlib

import pandas

from forml import testing
from forml.io import asset
from forml.pipeline import payload


class TestTrainsetReturn(testing.operator(payload.TrainsetReturn)):
    """TrainsetReturn operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})
    LABELS = pandas.Series([0, 1, 0], name='baz')

    apply_mode = testing.Case().apply(FEATURES).raises(RuntimeError, 'not trained')
    train_mode = (
        testing.Case()
        .train(FEATURES, LABELS)
        .apply(FEATURES)
        .returns(FEATURES.set_index(LABELS.rename('Label')).reset_index(), testing.pandas_equals)
    )


class TestDump(testing.operator(payload.Dump)):
    """Dump operator unit tests."""

    FEATURES = pandas.DataFrame({'foo': [1.0, 2.0, 3.0], 'bar': ['a', 'b', 'b']})
    LABELS = pandas.Series([0, 1, 0], name='baz')
    TMP_DIR = asset.mkdtemp()

    def match_csv(  # pylint: disable=no-self-argument
        expected: tuple[str, pandas.DataFrame, pandas.DataFrame],
        actual: pandas.DataFrame,
    ) -> bool:
        """Custom matcher that compares the operator output as well as the content of the dumped csv file.

        Args:
            expected: Tuple of the csv file path, its expected content and the expected operator output.
            actual: The actual operator output.

        Returns:
            True if all matching.
        """
        expected_path, expected_content, expected_output = expected  # pylint: disable=unpacking-non-sequence
        return testing.pandas_equals(expected_output, actual) and testing.pandas_equals(
            expected_content, pandas.read_csv(expected_path)
        )

    no_path = testing.Case().raises(TypeError, 'Path is required')
    apply_no_placeholders = (
        testing.Case(path=TMP_DIR / 'test.csv')
        .apply(FEATURES)
        .returns((TMP_DIR / 'test.csv', FEATURES, FEATURES), match_csv)
    )
    apply_with_placeholder = (
        testing.Case(path=TMP_DIR / 'test' / '$mode-$seq.csv')
        .apply(FEATURES)
        .returns((TMP_DIR / 'test' / 'apply-1.csv', FEATURES, FEATURES), match_csv)
    )
    train_with_subdir = (
        testing.Case(path=TMP_DIR / '$mode' / 'test-$seq.csv')
        .train(FEATURES, LABELS)
        .apply(FEATURES)
        .returns(
            (TMP_DIR / 'train' / 'test-1.csv', FEATURES.set_index(LABELS.rename('Label')).reset_index(), FEATURES),
            match_csv,
        )
    )

    def setUp(self):
        """Cleanup the temp dir before each scenario."""

        def rmtree(folder: pathlib.Path):
            """Recursively delete content of the folder (not the folder itself)."""
            for item in folder.iterdir():
                if item.is_dir():
                    rmtree(item)
                    item.rmdir()
                else:
                    item.unlink()

        rmtree(self.TMP_DIR)
