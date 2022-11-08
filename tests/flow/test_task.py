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
import typing

import cloudpickle
import pytest

from forml import flow
from forml.io import layout


class TestActor:
    """Actor unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def instance(
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]
    ) -> flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]:
        """Instance fixture."""
        return actor_builder()

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
        assert instance.apply(testset) == actor_prediction
        assert instance.get_state() == actor_state
        instance.train('foo', 'bar')  # retraining to change the state
        assert instance.apply(testset) != actor_prediction
        assert 'x' not in instance.get_params()
        instance.set_params(x=100)
        instance.set_state(actor_state)
        assert instance.get_params()['x'] == 100  # state shouldn't override parameter setting

    def test_builder(
        self,
        actor_type: type[flow.Actor],
        hyperparams: typing.Mapping[str, int],
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    ):
        """Test the builder creation of the actor class."""
        assert actor_type.builder(**hyperparams) == actor_builder

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
        actor = cloudpickle.loads(cloudpickle.dumps(instance))
        assert actor.apply(testset) == actor_prediction


class TestBuilder:
    """Task builder unit tests."""

    def test_serializable(
        self,
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    ):
        """Test builder serializability."""
        assert cloudpickle.loads(cloudpickle.dumps(actor_builder)).actor == actor_builder.actor

    def test_instantiate(self, actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]):
        """Testing builder to actor instantiation."""
        assert actor_builder(b=3).get_params() == actor_builder.kwargs | {'b': 3}

    def test_repr(self, actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]):
        """Test the builder repr string."""
        assert repr(actor_builder) == flow.name(actor_builder.actor, *actor_builder.args, **actor_builder.kwargs)
