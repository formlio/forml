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
Generic operators unit tests.
"""
# pylint: disable=no-self-use

from forml.pipeline import wrap


class TestAdapter:
    """Adapter unit tests."""

    # pylint: disable=protected-access
    def test_noparams(self):
        """Test adapter setup using non-parametrized decorators."""

        @wrap.Adapter.train
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.spec()
        assert func._train.spec()().apply(None) == ('foo', {})
        assert func._label.spec()().apply(None) == ('bar', {})

    def test_params(self):
        """Test adapter setup using parametrized decorators."""

        @wrap.Adapter.train(foo='foo', bar='foo')
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.spec()
        assert func._train.spec(bar='bar')().apply(None) == ('foo', {'foo': 'foo', 'bar': 'bar'})

    def test_multi(self):
        """Test adapter setup using multiple decorators."""

        @wrap.Adapter.train(foo='foo')
        @wrap.Adapter.apply(bar='foo')
        @wrap.Adapter.label
        @wrap.Actor.apply
        def funcfoo(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        assert funcfoo._train.spec()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcfoo._apply.spec()().apply(None) == ('foo', {'bar': 'foo'})
        assert funcfoo._label.spec()().apply(None) == ('foo', {})

        @funcfoo.apply(bar='bar')
        @wrap.Actor.apply
        def funcbar(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert funcbar._train.spec()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcbar._apply.spec()().apply(None) == ('bar', {'bar': 'bar'})
        assert funcbar._label.spec()().apply(None) == ('foo', {})

    def test_setup(self):
        """Test the operator instantiation."""

        @wrap.Adapter.train(foo='foo', bar='bar')
        @wrap.Adapter.apply(foo='foo', bar='bar')
        @wrap.Adapter.label(foo='foo', bar='bar')
        @wrap.Actor.apply
        def func(**kwargs):
            """Dummy actor."""
            return 'foo', kwargs

        operator = func(bar='baz')
        # pylint: disable=no-member
        assert (
            operator._apply.kwargs == operator._train.kwargs == operator._label.kwargs == {'foo': 'foo', 'bar': 'baz'}
        )
