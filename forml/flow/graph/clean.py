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
Graph topology validation.

# train and apply graph don't intersect
# acyclic
# train path is a closure
# apply path is a channel
# nodes ports subscriptions:
# * train/apply subscriptions are exclusive (enforced synchronously)
# * no future nodes
# * apply channel has no sinks
# * at most single trained node per each instance (enforced synchronously)
# * either both train and label or all apply inputs and outputs are active
"""
import typing

from forml.flow import error
from forml.flow.graph import view, node as grnode


class Validator(view.Visitor):
    """Visitor ensuring all nodes are in valid state which means:

    * are Worker instances (not Future)
    * have consistent subscriptions:
        * only both train and label or all input ports
        * only both train and label or all output ports
    """

    def __init__(self):
        self._futures: typing.Set[grnode.Atomic] = set()

    def visit_node(self, node: grnode.Atomic) -> None:
        """Node visit.

        Args:
            node: Node to be visited.
        """
        if isinstance(node, grnode.Future):
            self._futures.add(node)

    def visit_path(self, path: view.Path) -> None:
        """Final visit.

        Args:
            path: Path to be visited.
        """
        if self._futures:
            raise error.Topology(f'Future nodes on path: {", ".join(str(f) for f in self._futures)}')
