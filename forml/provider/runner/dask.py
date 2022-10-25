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
Dask runner.
"""
import logging
import typing

import dask
import distributed
from dask.delayed import Delayed

from forml import flow, runtime

if typing.TYPE_CHECKING:
    from forml import io
    from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Runner(runtime.Runner, alias='dask'):
    """ForML runner implementation using the :doc:`Dask computing library <dask:index>` as the
    execution platform.

    Args:
        kwargs: Any :doc:`Dask Configuration options <dask:configuration>`.

                Noteworthy parameters:
                   * ``scheduler`` selects the :doc:`scheduling implementation <dask:scheduling>`
                     (valid options are: ``synchronous``, ``threads``, ``processes``,
                     ``distributed``)
                   * to submit to a remote :doc:`Dask Cluster <distributed:index>`, set the
                     ``scheduler`` to ``distributed`` and provide the master ``scheduler-address``

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [RUNNER.compute]
        provider = "dask"
        scheduler = "processes"

    Important:
        Select the ``dask`` :ref:`extras to install <install-extras>` ForML together with the Dask
        support.
    """

    DEFAULTS = {
        'scheduler': 'processes',
    }

    def __init__(
        self,
        instance: typing.Optional['asset.Instance'] = None,
        feed: typing.Optional['io.Feed'] = None,
        sink: typing.Optional['io.Sink'] = None,
        **kwargs,
    ):
        super().__init__(instance, feed, sink)
        dask.config.set(self.DEFAULTS | kwargs)
        self._client: typing.Optional[distributed.Client] = None

    def start(self) -> None:
        if dask.config.get('scheduler') == 'distributed':
            self._client = distributed.Client()
            self._client.start()

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @staticmethod
    def _mkjob(symbols: typing.Collection[flow.Symbol]) -> typing.Iterable[Delayed]:
        """Construct the linked task graph in Dask representation.

        Args:
            symbols: Internal DAG representation in form of the compiled symbols.

        Returns:
            Leaf nodes of the constructed DAG.
        """

        def link(leaf: flow.Instruction) -> Delayed:
            """Recursive linking the given leaf to its upstream branch.

            Args:
                leaf: The leaf node to be linked upstream.

            Returns:
                The leaf node linked to its upstream branch.
            """
            if leaf not in branches:
                branches[leaf] = dask.delayed(leaf, pure=True, traverse=False)(*(link(a) for a in args.get(leaf, [])))
            return branches[leaf]

        args: typing.Mapping[flow.Instruction, typing.Sequence[flow.Instruction]] = dict(symbols)
        assert len(args) == len(symbols), 'Duplicated symbols in DAG sequence'
        leaves = set(args).difference(p for a in args.values() for p in a)
        assert leaves, 'Not acyclic'
        branches: dict[flow.Instruction, Delayed] = {}
        return (link(d) for d in leaves)

    @classmethod
    def run(cls, symbols: typing.Collection[flow.Symbol], **kwargs) -> None:
        dask.compute(cls._mkjob(symbols))
