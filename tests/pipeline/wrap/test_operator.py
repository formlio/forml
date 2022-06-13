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
Operator wrapper unit tests.
"""
# pylint: disable=no-self-use
import typing

from forml import testing
from forml.pipeline import wrap


@wrap.Actor.apply
def stateless(features: str) -> str:
    """Stateless actor."""
    return features.upper()


@wrap.Actor.train
def stateful(state: typing.Optional[str], features: str, labels: str, arg='') -> str:  # pylint: disable=unused-argument
    """Stateful actor train mode."""
    return features.upper() + labels + arg


@stateful.apply
def stateful(state: str, features: str, arg='') -> str:
    """Stateful actor apply mode."""
    return state + features.upper() + arg


class TestOperator(testing.operator(wrap.Operator)):
    """Operator unit tests."""

    # label
    test_stateful_label = testing.Case(label=stateful.builder()).raises(TypeError, message='actor invalid')
    test_stateless_label = (
        testing.Case(train=stateful.builder(), label=stateless.builder()).train('foo', 'bar').returns('FOOBARFOO')
    )

    # stateless apply/train
    test_stateless_apply = testing.Case(apply=stateless.builder()).apply('foo').returns('FOO')
    test_stateless_train_returns = testing.Case(train=stateless.builder()).train('foo', 'bar').returns('FOO')
    test_stateless_train_apply = testing.Case(train=stateless.builder()).train('foo', 'bar').apply('baz').returns('baz')

    # consumer
    test_consumer_returns = testing.Case(apply=stateful.builder()).train('foo', 'bar').returns('foo')
    test_consumer_apply = testing.Case(apply=stateful.builder()).train('foo', 'bar').apply('baz').returns('FOObarBAZ')

    # fit_transform
    test_fittransform_returns = testing.Case(train=stateful.builder()).train('foo', 'bar').returns('FOObarFOO')
    test_fittransform_apply = testing.Case(train=stateful.builder()).train('foo', 'bar').apply('baz').returns('baz')

    # mapper
    test_mapper_returns = (
        testing.Case(apply=stateful.builder(), train=stateful.builder()).train('foo', 'bar').returns('FOObarFOO')
    )
    test_mapper_apply = (
        testing.Case(apply=stateful.builder(), train=stateful.builder())
        .train('foo', 'bar')
        .apply('baz')
        .returns('FOObarBAZ')
    )

    # independent
    test_independent_returns = (
        testing.Case(apply=stateful.builder(arg='xxx'), train=stateful.builder(arg='yyy'))
        .train('foo', 'bar')
        .returns('FOObaryyyFOOyyy')
    )
    test_independent_apply = (
        testing.Case(apply=stateful.builder(arg='xxx'), train=stateful.builder(arg='yyy'))
        .train('foo', 'bar')
        .apply('baz')
        .returns('FOObarxxxBAZxxx')
    )

    # pylint: disable=protected-access
    def test_noparams(self):
        """Test adapter setup using non-parametrized decorators."""

        @wrap.Operator.train
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.builder()
        assert func._train.builder()().apply(None) == ('foo', {})
        assert func._label.builder()().apply(None) == ('bar', {})

    def test_params(self):
        """Test adapter setup using parametrized decorators."""

        @wrap.Operator.train(foo='foo', bar='foo')
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        @func.label
        @wrap.Actor.apply
        def func(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert not func._apply.builder()
        assert func._train.builder(bar='bar')().apply(None) == ('foo', {'foo': 'foo', 'bar': 'bar'})

    def test_multi(self):
        """Test adapter setup using multiple decorators."""

        @wrap.Operator.train(foo='foo')
        @wrap.Operator.apply(bar='foo')
        @wrap.Operator.label
        @wrap.Actor.apply
        def funcfoo(_, **kw):
            """Dummy actor."""
            return 'foo', kw

        assert funcfoo._train.builder()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcfoo._apply.builder()().apply(None) == ('foo', {'bar': 'foo'})
        assert funcfoo._label.builder()().apply(None) == ('foo', {})

        @funcfoo.apply(bar='bar')
        @wrap.Actor.apply
        def funcbar(_, **kw):
            """Dummy actor."""
            return 'bar', kw

        assert funcbar._train.builder()().apply(None) == ('foo', {'foo': 'foo'})
        assert funcbar._apply.builder()().apply(None) == ('bar', {'bar': 'bar'})
        assert funcbar._label.builder()().apply(None) == ('foo', {})

    def test_setup(self):
        """Test the operator instantiation."""

        @wrap.Operator.train(foo='foo', bar='bar')
        @wrap.Operator.apply(foo='foo', bar='bar')
        @wrap.Operator.label(foo='foo', bar='bar')
        @wrap.Actor.apply
        def func(**kwargs):
            """Dummy actor."""
            return 'foo', kwargs

        operator = func(bar='baz')
        # pylint: disable=no-member
        assert (
            operator._apply.kwargs == operator._train.kwargs == operator._label.kwargs == {'foo': 'foo', 'bar': 'baz'}
        )
