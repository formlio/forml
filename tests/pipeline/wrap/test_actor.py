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
# pylint: disable=no-self-use
import typing

import pytest

from forml import flow
from forml.pipeline import wrap


class TestClass:
    """Wrapped class unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def actor() -> type[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor fixture."""

        @wrap.Actor.type(train='fit', apply=lambda r, f: r.predict(f) or 'N/A')
        class Replace:
            """Actor wrapped class."""

            get_params = set_params = lambda: None

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

                fit = predict = get_params = lambda: None

    def test_actor(self, actor: type[flow.Actor]):
        """Stateless actor test."""
        actor = actor(case=True)
        actor.train('foo', 'bar')
        actor.train('Blah', 'baz')
        assert actor.apply('foo') == 'bar'
        assert actor.apply('Blah') == 'baz'
        assert actor.apply('blah') == 'N/A'


class TestStateless:
    """Wrapped stateless function unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def actor() -> type[flow.Actor[str, None, str]]:
        """Actor fixture."""

        @wrap.Actor.apply
        def replace(string: str, *, old: str, new: str, count=-1):
            """Actor wrapped function."""
            return string.replace(old, new, count)

        return replace

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

    def test_actor(self, actor: type[flow.Actor]):
        """Actor apply test."""
        assert actor(old='baz', new='foo').apply('baz bar') == 'foo bar'


class TestStateful:
    """Wrapped stateful function unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def actor() -> type[flow.Actor[str, str, typing.Optional[str]]]:
        """Actor fixture."""

        @wrap.Actor.train
        def replace(
            state: typing.Optional[dict[str, str]], features: str, labels: str, case: bool = False
        ) -> dict[str, str]:
            """Actor wrapped function."""
            if not case:
                features = features.lower()
            if not state:
                state = {}
            state[features] = labels
            return state

        @replace.apply
        def replace(state: dict[str, str], features: str, case: bool = False) -> typing.Optional[str]:
            if not case:
                features = features.lower()
            return state.get(features, None)

        return replace

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

    def test_actor(self, actor: type[flow.Actor]):
        """Stateless actor test."""
        actor = actor(case=True)
        actor.train('foo', 'bar')
        actor.train('Blah', 'baz')
        assert actor.apply('foo') == 'bar'
        assert actor.apply('Blah') == 'baz'
        assert actor.apply('blah') is None
