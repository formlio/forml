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
Flow task unit tests.
"""
# pylint: disable=no-self-use
import pickle
import typing

import pytest

from forml import flow
from forml.io import layout


class TestActor:
    """Actor unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def instance(
        actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]
    ) -> flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]:
        """Instance fixture."""
        return actor_spec()

    def test_train(
        self,
        instance: flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor],
        trainset_features: layout.RowMajor,
        trainset_labels: layout.Array,
        testset: layout.RowMajor,
        actor_prediction: layout.RowMajor,
    ):
        """Test actor training."""
        assert instance.is_stateful()
        instance.train(trainset_features, trainset_labels)
        assert instance.apply(testset) == actor_prediction

    def test_params(
        self,
        instance: flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor],
        hyperparams: typing.Mapping[str, str],
    ):
        """Params setter/getter tests."""
        orig = instance.get_params()
        assert orig == hyperparams
        assert 'x' not in orig
        instance.set_params(x=100)
        assert instance.get_params()['x'] == 100

    def test_state(
        self,
        instance: flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor],
        trainset_features: layout.RowMajor,
        trainset_labels: layout.Array,
        actor_state: bytes,
        testset: layout.RowMajor,
        actor_prediction: layout.RowMajor,
    ):
        """Testing actor statefulness."""
        instance.train(trainset_features, trainset_labels)
        assert instance.predict(testset) == actor_prediction
        assert instance.get_state() == actor_state
        instance.train('foo', 'bar')  # retraining to change the state
        assert instance.predict(testset) != actor_prediction
        assert 'x' not in instance.get_params()
        instance.set_params(x=100)
        instance.set_state(actor_state)
        assert instance.get_params()['x'] == 100  # state shouldn't override parameter setting

    def test_spec(
        self,
        actor_type: type[flow.Actor],
        hyperparams: typing.Mapping[str, int],
        actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    ):
        """Test the spec creation of the actor class."""
        assert actor_type.spec(**hyperparams) == actor_spec

    def test_serializable(
        self,
        instance: flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor],
        trainset_features: layout.RowMajor,
        trainset_labels: layout.Array,
        testset: layout.RowMajor,
        actor_prediction: layout.RowMajor,
    ):
        """Test actor serializability."""
        instance.train(trainset_features, trainset_labels)
        assert pickle.loads(pickle.dumps(instance)).predict(testset) == actor_prediction


class TestSpec:
    """Task spec unit tests."""

    def test_serializable(
        self,
        actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
        actor_type: type[flow.Actor],
    ):
        """Test spec serializability."""
        assert pickle.loads(pickle.dumps(actor_spec)).actor == actor_type

    def test_instantiate(self, actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]):
        """Testing specto actor instantiation."""
        assert actor_spec(b=3).get_params() == {**actor_spec.kwargs, 'b': 3}
