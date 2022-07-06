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
Runtime that just renders the pipeline DAG visualization.
"""
import logging
import pathlib
import typing

import graphviz as grviz

from forml import conf, flow, io, runtime
from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Runner(runtime.Runner, alias='graphviz'):
    """(Pseudo)runner using the :doc:`Graphviz drawing software <graphviz:index>` for rendering
    graphical visualization of the workflow task graph.

    For better readability, the runner is using the following shapes to plot the different objects:

    ============= ===========================================
        Shape       Meaning
    ============= ===========================================
     Square box     Actor in train mode.
     Round box      Actor in apply mode.
     Ellipse        System actor for output port selection.
     Cylinder       System actor for state persistence.
     Solid edge     Data transfer.
     Dotted edge    State transfer.
    ============= ===========================================

    The workflow obviously doesn't get really executed!

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [RUNNER.visual]
        provider = "graphviz"
        format = "svg"
        engine = "dot"
        graph_attr = { rankdir = "LR", "bgcolor" = "transparent" }
        node_attr = {  }

    Important:
        Select the ``graphviz`` :ref:`extras to install <install-extras>` ForML together with the
        Graphviz support. Additionally, download and install also the `native Graphviz system
        binary <https://www.graphviz.org/download/>`_ (OS specific procedure).
    """

    FILEPATH = f'{conf.APPNAME}.dot'
    OPTIONS = {}

    def __init__(
        self,
        instance: typing.Optional[asset.Instance] = None,
        feed: typing.Optional[io.Feed] = None,
        sink: typing.Optional[io.Sink] = None,
        filepath: typing.Optional[typing.Union[str, pathlib.Path]] = None,
        view: bool = True,
        **options: typing.Any,
    ):
        """
        Args:
            filepath: Target path for producing the DOT file.
            view: If True, open the rendered result with the default application.
            options: Any of the supported (and non-colliding) :class:`graphviz.Digraph` keyword
                     arguments.
        """
        super().__init__(instance, feed, sink)
        self._filepath: pathlib.Path = pathlib.Path(filepath or self.FILEPATH)
        self._view: bool = view
        self._options: typing.Mapping[str, typing.Any] = self.OPTIONS | options

    def _run(self, symbols: typing.Collection[flow.Symbol]) -> None:
        dot: grviz.Digraph = grviz.Digraph(**self._options)
        for sym in symbols:
            nodekw = dict(shape='ellipse')
            outkw = dict(style='solid')
            if isinstance(sym.instruction, flow.Functor):
                nodekw.update(shape='box')
                if flow.Train not in sym.instruction.action:
                    nodekw.update(style='rounded')
            elif isinstance(sym.instruction, (flow.Loader, flow.Dumper, flow.Committer)):
                nodekw.update(shape='cylinder')
                outkw.update(style='dotted')
            dot.node(repr(id(sym.instruction)), repr(sym.instruction), **nodekw)
            for idx, arg in enumerate(sym.arguments):
                inkw = dict(outkw)
                if isinstance(arg, (flow.Loader, flow.Dumper)) or (
                    isinstance(arg, flow.Functor) and flow.Train in arg.action
                ):
                    inkw.update(style='dotted')
                dot.edge(repr(id(arg)), repr(id(sym.instruction)), label=repr(idx), **inkw)
        dot.render(self._filepath, view=self._view)
