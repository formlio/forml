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
ForML runtime instruction unit tests.
"""
# pylint: disable=no-self-use
import abc
import pickle
import typing

import pytest

from forml import flow
from forml.runtime.code._target import user


class Functor(metaclass=abc.ABCMeta):
    """Common functor tests."""

    @staticmethod
    @abc.abstractmethod
    def functor(spec: flow.Spec) -> user.Functor:
        """Functor fixture."""

    def test_serializable(self, functor: user.Functor, state: bytes, args: typing.Sequence):
        """Test functor serializability."""
        functor = functor.preset_state()
        output = functor(state, *args)
        clone = pickle.loads(pickle.dumps(functor))
        assert isinstance(clone, user.Functor)
        assert functor(state, *args) == output


class TestMapper(Functor):
    """Mapper functor unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: flow.Spec) -> user.Functor:
        """Functor fixture."""
        return user.Mapper().functor(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(testset) -> typing.Sequence:
        """Functor args fixture."""
        return [testset]

    def test_call(self, functor: user.Functor, state: bytes, hyperparams, testset, prediction):
        """Test the functor call."""
        with pytest.raises(ValueError):
            functor(testset)
        functor = functor.preset_state()
        assert functor(state, testset) == prediction
        functor = functor.preset_params()
        assert functor(hyperparams, state, testset) == prediction


class TestTrainer(Functor):
    """Trainer functor unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def functor(spec: flow.Spec) -> user.Functor:
        """Functor fixture."""
        return user.Trainer().functor(spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(trainset, testset) -> typing.Sequence:
        """Functor args fixture."""
        return [trainset, testset]

    def test_call(self, functor: user.Functor, state: bytes, hyperparams, trainset):
        """Test the functor call."""
        assert functor(*trainset) == state
        functor = functor.preset_params()
        assert functor(hyperparams, *trainset) == state
