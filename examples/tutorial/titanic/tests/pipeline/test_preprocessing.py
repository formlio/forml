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
Preprocessing unit tests.

We are thoroughly testing our preprocessing transformers.
"""
import numpy
import pandas
from titanic.pipeline import preprocessing

from forml import testing


class TestParseTitle(testing.operator(preprocessing.parse_title)):
    """Unit testing the stateless TitleParser transformer."""

    # Dataset fixtures
    INPUT = pandas.DataFrame({'Name': ['Smith, Mr. John', 'Black, Ms. Jane', 'Brown, Mrs. Jo', 'White, Ian']})
    EXPECTED = pandas.DataFrame({'Title': ['mr', 'ms', 'mrs', 'n/a']})

    # Test scenarios
    invalid_params = testing.Case(foo='bar').raises(TypeError, 'missing a required argument:')
    invalid_source = testing.Case(source='Foo', target='Bar').apply(INPUT).raises(KeyError, 'Foo')
    valid_parsing = testing.Case(source='Name', target='Title').apply(INPUT).returns(EXPECTED, testing.pandas_equals)


class TestImpute(testing.operator(preprocessing.impute)):
    """NaN Imputer unit tests."""

    def matcher(expected: pandas.DataFrame, actual: pandas.DataFrame) -> bool:  # pylint: disable=no-self-argument
        """Custom matcher to verify the actual imputations."""
        assert actual.notna().all().all()
        # pylint: disable=unsubscriptable-object
        if not testing.pandas_equals(expected['Embarked'], actual['Embarked']) or not testing.pandas_equals(
            expected['Fare'], actual['Fare']
        ):
            return False
        source_age = TestImpute.FEATURES['Age']
        actual_age = actual['Age']
        return source_age.mean() == actual_age.mean() and source_age.std() == actual_age.std()

    # Dataset fixtures
    FEATURES = pandas.DataFrame(
        {'Age': [1.0, numpy.nan, 3.0], 'Embarked': ['X', numpy.nan, 'Y'], 'Fare': [1.0, numpy.nan, 3.0]}
    )
    EXPECTED = pandas.DataFrame({'foo': [1.0, 4.0, 2.0], 'Embarked': ['X', 'S', 'Y'], 'Fare': [1.0, 2.0, 3.0]})

    # Test scenarios
    invalid_params = testing.Case('foo').raises(TypeError, 'takes 1 positional argument but 2 were given')
    not_trained = testing.Case().apply(FEATURES).raises(RuntimeError, 'not trained')
    valid_imputation = testing.Case().train(FEATURES).apply(FEATURES).returns(EXPECTED, matcher)
