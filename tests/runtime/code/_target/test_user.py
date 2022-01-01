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
from forml.io import layout
from forml.runtime.code._target import user


class Functor(metaclass=abc.ABCMeta):
    """Common functor tests."""

    @staticmethod
    @abc.abstractmethod
    def functor(actor_spec: flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor]) -> user.Functor:
        """Functor fixture."""

    def test_serializable(self, functor: user.Functor, actor_state: bytes, args: typing.Sequence):
        """Test functor serializability."""
        functor = functor.preset_state()
        output = functor(actor_state, *args)
        clone = pickle.loads(pickle.dumps(functor))
        assert isinstance(clone, user.Functor)
        assert functor(actor_state, *args) == output


class TestApply(Functor):
    """Mapper functor unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def functor(actor_spec: flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor]) -> user.Functor:
        """Functor fixture."""
        return user.Apply().functor(actor_spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(testset) -> typing.Sequence:
        """Functor args fixture."""
        return [testset]

    def test_call(
        self,
        functor: user.Functor,
        actor_state: bytes,
        hyperparams: typing.Mapping[str, str],
        testset: layout.RowMajor,
        actor_prediction: layout.RowMajor,
    ):
        """Test the functor call."""
        with pytest.raises(ValueError):
            functor(testset)
        functor = functor.preset_state()
        assert functor(actor_state, testset) == actor_prediction
        functor = functor.preset_params()
        assert functor(hyperparams, actor_state, testset) == actor_prediction


class TestTrain(Functor):
    """Trainer functor unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def functor(actor_spec: flow.Spec[layout.RowMajor, layout.Array, layout.RowMajor]) -> user.Functor:
        """Functor fixture."""
        return user.Train().functor(actor_spec)

    @staticmethod
    @pytest.fixture(scope='session')
    def args(trainset_features: layout.RowMajor, trainset_labels: layout.Array) -> typing.Sequence:
        """Functor args fixture."""
        return trainset_features, trainset_labels

    def test_call(
        self,
        functor: user.Functor,
        actor_state: bytes,
        hyperparams: typing.Mapping[str, str],
        trainset_features: layout.RowMajor,
        trainset_labels: layout.Array,
    ):
        """Test the functor call."""
        assert functor(trainset_features, trainset_labels) == actor_state
        functor = functor.preset_params()
        assert functor(hyperparams, trainset_features, trainset_labels) == actor_state
