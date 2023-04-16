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
Spark runner.
"""
import logging
import typing

import pyspark

from forml import flow, runtime

if typing.TYPE_CHECKING:
    from forml import io
    from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Runner(runtime.Runner, alias='spark'):
    """ForML runner utilizing :doc:`Apache Spark <pyspark:index>` as a distributed executor.

    Args:
        kwargs: Any `Spark Configuration options
                <https://spark.apache.org/docs/latest/configuration.html>`_.

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [RUNNER.compute]
        provider = "spark"
        "spark.driver.cores" = 1
        "spark.driver.memory" = "1g"
        "spark.executor.cores" = 2
        "spark.executor.memory" = "1g"
        "spark.executor.pyspark.memory" = "1g"

    Important:
        Select the ``spark`` :ref:`extras to install <install-extras>` ForML together with the Spark
        support.

    Note:
        ForML uses Spark purely as an *executor* without any deeper integration with its robust data
        management API.
    """

    DEFAULTS = {'spark.app.name': 'ForML'}

    def __init__(
        self,
        instance: typing.Optional['asset.Instance'] = None,
        feed: typing.Optional['io.Feed'] = None,
        sink: typing.Optional['io.Sink'] = None,
        **kwargs,
    ):
        super().__init__(instance, feed, sink)
        self._config: pyspark.SparkConf = pyspark.SparkConf().setAll((self.DEFAULTS | kwargs).items())
        self._context: typing.Optional[pyspark.SparkContext] = None

    def start(self) -> None:
        self._context = pyspark.SparkContext.getOrCreate(self._config)

    def close(self) -> None:
        self._context.stop()
        self._context = None

    @staticmethod
    def _submit(spark: pyspark.SparkContext, symbols: typing.Collection[flow.Symbol]) -> typing.Iterable[pyspark.RDD]:
        """Build and submit the task graph in Spark representation.

        Args:
            symbols: Internal DAG representation in form of the compiled symbols.

        Returns:
            Leaf nodes of the constructed DAG.
        """

        def apply(instruction: flow.Instruction, *args: pyspark.RDD) -> pyspark.RDD:
            """Perform the instruction using the given RDDs as arguments.

            Args:
                instruction: Flow instruction to be performed.
                *args: RDDs to be used as arguments.

            Returns:
                Result in form of a RDD.
            """
            if not args:
                return spark.parallelize([instruction()])
            if len(args) == 1:
                return args[0].map(instruction)
            return spark.parallelize([instruction(*(a.collect()[0] for a in args))])

        def link(leaf: flow.Instruction) -> pyspark.RDD:
            """Recursive linking the given leaf to its upstream branch.

            Args:
                leaf: The leaf node to be linked upstream.

            Returns:
                The leaf node linked to its upstream branch.
            """
            if leaf not in nodes:
                nodes[leaf] = apply(leaf, *(link(a) for a in arguments.get(leaf, [])))
            else:
                nodes[leaf].cache()
            return nodes[leaf]

        arguments: typing.Mapping[flow.Instruction, typing.Sequence[flow.Instruction]] = dict(symbols)
        assert len(arguments) == len(symbols), 'Duplicated symbols in DAG sequence'
        leaves = set(arguments).difference(p for a in arguments.values() for p in a)
        assert leaves, 'Not acyclic'
        nodes: dict[flow.Instruction, pyspark.RDD] = {}
        return (link(d) for d in leaves)

    @classmethod
    def run(cls, symbols: typing.Collection[flow.Symbol], **kwargs) -> None:
        for result in cls._submit(pyspark.SparkContext.getOrCreate(), symbols):
            result.collect()
