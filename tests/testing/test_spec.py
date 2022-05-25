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
ForML testing spec unit tests.
"""
# pylint: disable=protected-access,no-self-use

import typing

from forml import testing


class TestScenario:
    """Testing Case unit tests."""

    def test_init_raises(
        self, init_raises: testing.Scenario, hyperparams: typing.Mapping[str, int], exception: Exception
    ):
        """Scenario spec test case."""
        assert init_raises.outcome == testing.Scenario.Outcome.INIT_RAISES
        assert init_raises.params.kwargs == hyperparams
        assert init_raises.exception == exception

    def test_plainapply_raises(
        self,
        plainapply_raises: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        apply_input: str,
        exception: Exception,
    ):
        """Scenario spec test case."""
        assert plainapply_raises.outcome == testing.Scenario.Outcome.PLAINAPPLY_RAISES
        assert plainapply_raises.params.kwargs == hyperparams
        assert plainapply_raises.input.apply == apply_input
        assert plainapply_raises.exception == exception

    def test_plainapply_returns(
        self,
        plainapply_returns: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        apply_input: str,
        apply_output: str,
    ):
        """Scenario spec test case."""
        assert plainapply_returns.outcome == testing.Scenario.Outcome.PLAINAPPLY_RETURNS
        assert plainapply_returns.params.kwargs == hyperparams
        assert plainapply_returns.input.apply == apply_input
        assert plainapply_returns.output.apply == apply_output

    def test_statetrain_raises(
        self,
        statetrain_raises: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        train_input: str,
        label_input: str,
        exception: Exception,
    ):
        """Scenario spec test case."""
        assert statetrain_raises.outcome == testing.Scenario.Outcome.STATETRAIN_RAISES
        assert statetrain_raises.params.kwargs == hyperparams
        assert statetrain_raises.input.train == train_input
        assert statetrain_raises.input.label == label_input
        assert statetrain_raises.exception == exception

    def test_statetrain_returns(
        self,
        statetrain_returns: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        train_input: str,
        label_input: str,
        train_output: str,
    ):
        """Scenario spec test case."""
        assert statetrain_returns.outcome == testing.Scenario.Outcome.STATETRAIN_RETURNS
        assert statetrain_returns.params.kwargs == hyperparams
        assert statetrain_returns.input.train == train_input
        assert statetrain_returns.input.label == label_input
        assert statetrain_returns.output.train[0] == train_output

    def test_stateapply_raises(
        self,
        stateapply_raises: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        train_input: str,
        label_input: str,
        apply_input: str,
        exception: Exception,
    ):
        """Scenario spec test case."""
        assert stateapply_raises.outcome == testing.Scenario.Outcome.STATEAPPLY_RAISES
        assert stateapply_raises.params.kwargs == hyperparams
        assert stateapply_raises.input.train == train_input
        assert stateapply_raises.input.label == label_input
        assert stateapply_raises.input.apply == apply_input
        assert stateapply_raises.exception == exception

    def test_stateapply_returns(
        self,
        stateapply_returns: testing.Scenario,
        hyperparams: typing.Mapping[str, int],
        train_input: str,
        label_input: str,
        apply_input: str,
        apply_output: str,
    ):
        """Scenario spec test case."""
        assert stateapply_returns.outcome == testing.Scenario.Outcome.STATEAPPLY_RETURNS
        assert stateapply_returns.params.kwargs == hyperparams
        assert stateapply_returns.input.train == train_input
        assert stateapply_returns.input.label == label_input
        assert stateapply_returns.input.apply == apply_input
        assert stateapply_returns.output.apply == apply_output
