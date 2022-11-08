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
Wrapped actor unit tests.
"""
import abc
import typing

import cloudpickle
import pytest

from forml import flow
from forml.pipeline import wrap


class Type(abc.ABC):
    """Common actor type tests."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def origin() -> typing.Any:
        """Origin implementation fixture."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def builder(origin: typing.Any) -> flow.Builder[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor builder fixture."""

    @staticmethod
    @pytest.fixture(scope='session')
    def actor(
        builder: flow.Builder[flow.Actor[str, str, typing.Optional[str]]]
    ) -> type[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor type fixture."""
        return builder.actor

    @staticmethod
    @pytest.fixture(scope='function')
    def instance(
        builder: flow.Builder[flow.Actor[str, str, typing.Optional[str]]]
    ) -> flow.Actor[str, str, typing.Optional[str]]:
        """Actor instance fixture."""
        return builder()

    def test_serializable(self, actor: type[flow.Actor], instance: flow.Actor):
        """Serializability test."""
        for subj, check in (actor, issubclass), (instance, isinstance):
            serde = cloudpickle.loads(cloudpickle.dumps(subj))
            assert hasattr(serde, 'apply')
            assert hasattr(serde, 'train')
            assert check(serde, flow.Actor)

    def test_type(self, actor: type[flow.Actor], instance: flow.Actor):
        """Actor type test."""
        assert issubclass(actor, flow.Actor)
        assert isinstance(instance, flow.Actor)
        assert isinstance(instance, actor)

    def test_docs(self, origin: typing.Any, actor: type[flow.Actor]):
        """Test the docstring propagation."""
        assert actor.__doc__ == origin.__doc__


class TestClass(Type):
    """Wrapped class unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def origin() -> typing.Any:
        """Origin implementation fixture."""

        class Replace:
            """Actor wrapped class."""

            get_params = lambda _: {}  # pylint: disable=unnecessary-lambda-assignment
            set_params = lambda _, **kw: None  # pylint: disable=unnecessary-lambda-assignment

            def __init__(self, case: bool = False):
                self._case: bool = case
                self._state: dict[str, str] = {}

            def fit(self, features: str, labels: str) -> None:
                """Train method."""
                if not self._case:
                    features = features.lower()
                self._state[features] = labels

            def predict(self, features: str) -> typing.Optional[str]:
                """Apply method."""
                if not self._case:
                    features = features.lower()
                return self._state.get(features, None)

        return Replace

    @staticmethod
    @pytest.fixture(scope='session')
    def builder(origin: typing.Any) -> flow.Builder[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor fixture."""

        return wrap.Actor.type(train='fit', apply=lambda r, f: r.predict(f) or 'N/A')(origin).builder(case=True)

    def test_invalid(self):
        """Invalid usage tests."""
        with pytest.raises(TypeError, match='Invalid actor class'):
            wrap.Actor.type(1)

        with pytest.raises(TypeError, match='Invalid mapping'):

            @wrap.Actor.type(train=1)
            class Foo:  # pylint: disable=unused-variable
                """Dummy."""

        with pytest.raises(TypeError, match='missing required set_params'):

            @wrap.Actor.type(train='fit', apply='predict')
            class Bar:  # pylint: disable=unused-variable
                """Dummy."""

                fit = predict = get_params = lambda: None  # pylint: disable=unnecessary-lambda-assignment

    def test_actor(self, instance: flow.Actor):
        """Stateless actor test."""
        instance.train('foo', 'bar')
        instance.train('Blah', 'baz')
        assert instance.apply('foo') == 'bar'
        assert instance.apply('Blah') == 'baz'
        assert instance.apply('blah') == 'N/A'


class TestStateless(Type):
    """Wrapped stateless function unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def origin() -> typing.Any:
        """Origin implementation fixture."""

        def replace(string: str, *, old: str, new: str, count=-1):
            """Actor wrapped function."""
            return string.replace(old, new, count)

        return replace

    @staticmethod
    @pytest.fixture(scope='session')
    def builder(origin: typing.Any) -> flow.Builder[flow.Actor[str, None, str]]:
        """Actor fixture."""
        return wrap.Actor.apply(origin).builder(old='baz', new='foo')

    def test_invalid(self):
        """Invalid usage tests."""
        with pytest.raises(TypeError, match='Invalid actor function'):
            wrap.Actor.apply(1)

    def test_signature(self, actor: type[flow.Actor]):
        """Actor signature test."""
        with pytest.raises(TypeError, match="missing a required argument: 'old'"):
            actor(new='bar')
        with pytest.raises(TypeError, match="got an unexpected keyword argument 'foo'"):
            actor(new='foo', old='asd', foo='bar')

    def test_actor(self, instance: flow.Actor):
        """Actor apply test."""
        assert instance.apply('baz bar') == 'foo bar'


class TestStateful(Type):
    """Wrapped stateful function unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def origin() -> typing.Any:
        """Origin implementation fixture."""

        def replace(state: dict[str, str], features: str, case: bool = False) -> typing.Optional[str]:
            """Actor wrapped function - apply part."""
            if not case:
                features = features.lower()
            return state.get(features, None)

        return replace

    @staticmethod
    @pytest.fixture(scope='session')
    def builder(origin: typing.Any) -> flow.Builder[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor fixture."""

        @wrap.Actor.train
        def replace(
            state: typing.Optional[dict[str, str]], features: str, labels: str, case: bool = False
        ) -> dict[str, str]:
            """Actor wrapped function - train part."""
            if not case:
                features = features.lower()
            if not state:
                state = {}
            state[features] = labels
            return state

        return replace.apply(origin).builder(case=True)

    def test_invalid(self, actor: type[flow.Actor]):
        """Invalid usage tests."""
        with pytest.raises(TypeError, match='Invalid actor function'):
            wrap.Actor.train(1).apply(1)
        with pytest.raises(RuntimeError, match='not trained'):
            actor().apply('foo')

    def test_signature(self, actor: type[flow.Actor]):
        """Actor signature test."""
        with pytest.raises(TypeError, match="got an unexpected keyword argument 'foo'"):
            actor(foo='bar')

    def test_actor(self, instance: flow.Actor):
        """Stateless actor test."""
        instance.train('foo', 'bar')
        instance.train('Blah', 'baz')
        assert instance.apply('foo') == 'bar'
        assert instance.apply('Blah') == 'baz'
        assert instance.apply('blah') is None
