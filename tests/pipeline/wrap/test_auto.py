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
Auto-wrapping unit tests.
"""
import abc
import sys
import typing

import pytest
from sklearn import base as skbase
from sklearn import ensemble, preprocessing

from forml import flow
from forml.pipeline import wrap
from forml.pipeline.wrap import _auto


class Wrapper(typing.Generic[_auto.Entity], abc.ABC):
    """Wrapper unit tests base class."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def wrapper() -> _auto.Auto:
        """Wrapper fixture."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def entity() -> _auto.Entity:
        """Subject fixture."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def mismatch() -> typing.Any:
        """Mismatching entity fixture."""

    def test_wrapper(self, wrapper: _auto.Auto, entity: _auto.Entity, mismatch: typing.Any):
        """Wrapper test."""
        assert not wrapper.match(mismatch)
        assert wrapper.match(entity)
        assert isinstance(wrapper(entity)(), flow.Operator)


class TestAutoSklearnTransformer(Wrapper[type[skbase.TransformerMixin]]):
    """Sklearn transformer wrapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def wrapper() -> wrap.AutoSklearnTransformer:
        return wrap.AutoSklearnTransformer()

    @staticmethod
    @pytest.fixture(scope='session')
    def entity() -> type[skbase.TransformerMixin]:
        return preprocessing.LabelEncoder

    @staticmethod
    @pytest.fixture(
        scope='session', params=('foo', ensemble.GradientBoostingClassifier, ensemble.GradientBoostingRegressor)
    )
    def mismatch(request) -> typing.Any:
        return request.param


class TestAutoSklearnClassifier(Wrapper[type[skbase.ClassifierMixin]]):
    """Sklearn classifier wrapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def wrapper() -> wrap.AutoSklearnClassifier:
        return wrap.AutoSklearnClassifier()

    @staticmethod
    @pytest.fixture(scope='session')
    def entity() -> type[skbase.ClassifierMixin]:
        return ensemble.GradientBoostingClassifier

    @staticmethod
    @pytest.fixture(scope='session', params=('foo', preprocessing.LabelEncoder, ensemble.GradientBoostingRegressor))
    def mismatch(request) -> typing.Any:
        return request.param


class TestAutoSklearnRegressor(Wrapper[type[skbase.RegressorMixin]]):
    """Sklearn regressor wrapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def wrapper() -> wrap.AutoSklearnRegressor:
        return wrap.AutoSklearnRegressor()

    @staticmethod
    @pytest.fixture(scope='session')
    def entity() -> type[skbase.RegressorMixin]:
        return ensemble.GradientBoostingRegressor

    @staticmethod
    @pytest.fixture(scope='session', params=('foo', ensemble.GradientBoostingClassifier, preprocessing.LabelEncoder))
    def mismatch(request) -> typing.Any:
        return request.param


def test_importer():
    """Autowrapping importer context manager unit test."""
    # pylint: disable=import-outside-toplevel,reimported
    with wrap.importer():
        from sklearn.ensemble import GradientBoostingClassifier
    assert isinstance(GradientBoostingClassifier(max_depth=5), flow.Operator)
    assert 'sklearn.ensemble' not in sys.modules
    from sklearn.ensemble import GradientBoostingClassifier

    assert not isinstance(GradientBoostingClassifier(max_depth=5), flow.Operator)

    with wrap.importer():
        from sklearn import ensemble
    assert isinstance(ensemble.GradientBoostingClassifier(max_depth=5), flow.Operator)
    assert 'sklearn.ensemble' not in sys.modules
    from sklearn import ensemble

    assert not isinstance(ensemble.GradientBoostingClassifier(max_depth=5), flow.Operator)

    with wrap.importer():
        import sklearn.ensemble
    assert isinstance(sklearn.ensemble.GradientBoostingClassifier(max_depth=5), flow.Operator)
    assert 'sklearn.ensemble' not in sys.modules
    import sklearn.ensemble

    assert not isinstance(sklearn.ensemble.GradientBoostingClassifier(max_depth=5), flow.Operator)
