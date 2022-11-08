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
import typing

from forml import testing
from forml.pipeline.wrap import _actor, _operator


@_actor.Actor.apply
def Stateless(features: str) -> str:  # pylint: disable=invalid-name
    """Stateless actor."""
    return features.upper()


@_actor.Actor.train
def Stateful(  # pylint: disable=invalid-name
    state: typing.Optional[str], features: str, labels: str, arg=''  # pylint: disable=unused-argument
) -> str:
    """Stateful actor train mode."""
    return features.upper() + labels + arg


@Stateful.apply
def Stateful(state: str, features: str, arg='') -> str:  # pylint: disable=invalid-name
    """Stateful actor apply mode."""
    return state + features.upper() + arg


class TestStatelessLabel(testing.operator(_operator.Operator.train(Stateful).label(Stateless))):
    """Testing an operator with a stateless label actor."""

    test_train = testing.Case().train('foo', 'bar').returns('FOOBARFOO')


class TestStatefulLabel(testing.operator(_operator.Operator.train(Stateful).label(Stateful))):
    """Testing an operator with a stateful label actor."""

    test_train = testing.Case().train('foo', 'bar').returns('FOOFOObarBARFOO')


class TestStatelessApply(testing.operator(_operator.Operator.apply(Stateless))):
    """Testing an operator with a stateless apply actor."""

    test_apply = testing.Case().apply('foo').returns('FOO')


class TestStatefulApply(testing.operator(_operator.Operator.apply(Stateful))):
    """Testing an operator with a stateful apply actor."""

    test_train = testing.Case().train('foo', 'bar').returns('foo')
    test_apply = testing.Case().train('foo', 'bar').apply('baz').returns('FOObarBAZ')


class TestStatelessTrain(testing.operator(_operator.Operator.train(Stateless))):
    """Testing an operator with a stateless train actor."""

    test_train = testing.Case().train('foo', 'bar').returns('FOO')
    test_apply = testing.Case().train('foo', 'bar').apply('baz').returns('baz')


class TestStatefulTrain(testing.operator(_operator.Operator.train(Stateful))):
    """Testing an operator with a stateful train actor."""

    test_train = testing.Case().train('foo', 'bar').returns('FOObarFOO')
    test_apply = testing.Case().train('foo', 'bar').apply('baz').returns('baz')


class TestMapper(testing.operator(_operator.Operator.apply(Stateful).train(Stateful))):
    """Testing a stateful mapper operator."""

    test_train = testing.Case().train('foo', 'bar').returns('FOObarFOO')
    test_apply = testing.Case().train('foo', 'bar').apply('baz').returns('FOObarBAZ')


class TestParams(testing.operator(_operator.Operator.apply(Stateful, arg='xxx').train(Stateful, arg='yyy'))):
    """Testing operator actor parametrization."""

    test_train_default = testing.Case().train('foo', 'bar').returns('FOObaryyyFOOyyy')
    test_train_custom = testing.Case(arg='zzz').train('foo', 'bar').returns('FOObarzzzFOOzzz')
    test_apply_default = testing.Case().train('foo', 'bar').apply('baz').returns('FOObarxxxBAZxxx')
    test_apply_custom = testing.Case(arg='zzz').train('foo', 'bar').apply('baz').returns('FOObarzzzBAZzzz')


def test_noparams():
    """Test operator setup using non-parametrized decorators."""

    @_operator.Operator.train
    @_actor.Actor.apply
    def Func(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'foo', kw

    @Func.label
    @_actor.Actor.apply
    def Func(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'bar', kw

    func = Func()  # pylint: disable=no-value-for-parameter
    assert not func.Apply  # pylint: disable=no-member
    assert func.Train().apply(None) == ('foo', {})  # pylint: disable=no-member
    assert func.Label().apply(None) == ('bar', {})  # pylint: disable=no-member


def test_params():
    """Test operator setup using parametrized decorators."""

    @_operator.Operator.train(foo='foo', bar='foo')
    @_actor.Actor.apply
    def Func(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'foo', kw

    @Func.label
    @_actor.Actor.apply
    def Func(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'bar', kw

    func = Func()  # pylint: disable=no-member,no-value-for-parameter
    assert not func.Apply  # pylint: disable=no-member
    assert func.Train(bar='bar').apply(None) == ('foo', {'foo': 'foo', 'bar': 'bar'})  # pylint: disable=no-member


def test_multi():
    """Test operator setup using multiple decorators."""

    @_operator.Operator.train(foo='foo')
    @_operator.Operator.apply(bar='foo')
    @_operator.Operator.label
    @_actor.Actor.apply
    def Funcfoo(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'foo', kw

    assert Funcfoo.Train().apply(None) == ('foo', {'foo': 'foo'})
    assert Funcfoo.Apply().apply(None) == ('foo', {'bar': 'foo'})
    assert Funcfoo.Label().apply(None) == ('foo', {})

    @Funcfoo.apply(bar='bar')
    @_actor.Actor.apply
    def Funcbar(_, **kw):  # pylint: disable=invalid-name
        """Dummy actor."""
        return 'bar', kw

    assert Funcbar.Train().apply(None) == ('foo', {'foo': 'foo'})
    assert Funcbar.Apply().apply(None) == ('bar', {'bar': 'bar'})
    assert Funcbar.Label().apply(None) == ('foo', {})


def test_docs():
    """Test the docstring propagation."""
    operator = _operator.Operator.apply(_operator.Operator.label(Stateless))
    assert operator.__doc__ == Stateless.__doc__
    operator = operator.train(Stateful)
    assert operator.__doc__ == Stateful.__doc__
