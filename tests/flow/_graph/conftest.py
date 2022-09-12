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
Graph unit tests fixtures.
"""

import pytest

from forml import flow


@pytest.fixture(scope='function')
def simple(actor_builder: flow.Builder) -> flow.Worker:
    """Simple node fixture with 1 input and 1 output apply port."""
    return flow.Worker(actor_builder, 1, 1)


@pytest.fixture(scope='function')
def multi(actor_builder: flow.Builder) -> flow.Worker:
    """Multi port node fixture (2 input and 2 output apply port)."""
    return flow.Worker(actor_builder, 2, 2)
