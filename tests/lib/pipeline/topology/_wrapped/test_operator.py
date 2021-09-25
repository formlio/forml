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

from forml.lib.pipeline import topology


class TestAdapter:
    """Adapter unit tests."""

    # pylint: disable=protected-access
    def test_noparams(self):
        """Test adapter setup using non-parametrized decorators."""

        @topology.Adapter.train
        @topology.Function.actor
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @topology.Function.actor
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.spec()
        assert func._train.spec()().apply(None) == ('foo', {})
        assert func._label.spec()().apply(None) == ('bar', {})

    def test_params(self):
        """Test adapter setup using parametrized decorators."""

        @topology.Adapter.train(foo='foo', bar='foo')
        @topology.Function.actor
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @topology.Function.actor(bar='bar', baz='bar')
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.spec()
        assert func._train.spec(bar='bar')().apply(None) == ('foo', {'foo': 'foo', 'bar': 'bar'})
        assert func._label.spec(baz='baz')().apply(None) == ('bar', {'bar': 'bar', 'baz': 'baz'})

    def test_multi(self):
        """Test adapter setup using multiple decorators."""

        @topology.Adapter.train(foo='foo')
        @topology.Adapter.apply(bar='foo')
        @topology.Adapter.label
        @topology.Function.actor
        def funcfoo(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        assert funcfoo._train.spec()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcfoo._apply.spec()().apply(None) == ('foo', {'bar': 'foo'})
        assert funcfoo._label.spec()().apply(None) == ('foo', {})

        @funcfoo.apply(bar='bar')
        @topology.Function.actor
        def funcbar(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert funcbar._train.spec()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcbar._apply.spec()().apply(None) == ('bar', {'bar': 'bar'})
        assert funcbar._label.spec()().apply(None) == ('foo', {})

    def test_setup(self):
        """Test the operator instantiation."""

        @topology.Adapter.train(foo='foo', bar='bar')
        @topology.Adapter.apply(foo='foo', bar='bar')
        @topology.Adapter.label(foo='foo', bar='bar')
        @topology.Function.actor
        def func(_, *args, **kwargs):
            """Dummy actor."""
            return 'foo', args, kwargs

        operator = func('foo', bar='baz')
        # pylint: disable=no-member
        assert operator._apply.args == operator._train.args == operator._label.args == ('foo',)
        assert (
            operator._apply.kwargs == operator._train.kwargs == operator._label.kwargs == {'foo': 'foo', 'bar': 'baz'}
        )
