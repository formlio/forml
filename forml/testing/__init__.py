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
Testing framework.
"""
import typing

from forml.flow.pipeline import topology
from forml.testing import spec, routine


def operator(subject: typing.Type[topology.Operator]) -> typing.Type[routine.Suite]:
    """Operator base class generator.

    Args:
        subject: Operator to be tested within given suite.
    """

    class Operator(routine.Suite, metaclass=routine.Meta):
        """Generated base class."""

        @property
        def __operator__(self) -> typing.Type[topology.Operator]:
            """Attached operator.

            Returns:
                Operator instance.
            """
            return subject

    return Operator


class Case(spec.Appliable):
    """Test case entrypoint."""

    def __init__(self, *args, **kwargs):
        super().__init__(spec.Scenario.Params(*args, **kwargs))

    def train(self, features: typing.Any, labels: typing.Any = None) -> spec.Trained:
        """Train input dataset definition."""
        return spec.Trained(self._params, spec.Scenario.Input(train=features, label=labels))
