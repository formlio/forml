"""
Runtime that just renders the pipeline DAG visualization.
"""
import collections
import logging
import typing
import uuid

import graphviz as grviz
from forml.runtime.code import instruction

from forml import etl
from forml.flow import task
from forml.flow.graph import view, node as grnode, port
from forml.runtime import code, process
from forml.runtime.asset import access

LOGGER = logging.getLogger(__name__)


class Dot(view.Visitor):
    """Path visitor for building the graphviz Dot structure.
    """
    def __init__(self, *args, **kwargs):
        self._dot: grviz.Digraph = grviz.Digraph(*args, **kwargs)
        self._titles: typing.Dict[task.Spec, typing.Dict[uuid.UUID, int]] = collections.defaultdict(dict)

    def visit_node(self, node: grnode.Worker) -> None:
        """Process new node.

        Args:
            node: Node to be processed.
        """
        self._dot.node(str(node.uid),
                       f'{node.spec}#{self._titles[node.spec].setdefault(node.gid, len(self._titles[node.spec]) + 1)}')
        for index, subscription in ((i, s) for i, p in enumerate(node.output) for s in p):
            self._dot.edge(str(node.uid), str(subscription.node.uid), label=f'{port.Apply(index)}->{subscription.port}')

    @property
    def source(self):
        """Return the graphviz source data structure.

        Returns: Graphviz source.
        """
        return self._dot.source

    def render(self, path: str):
        """Store the graphviz dot file.

        Args:
            path: Filesystem path for storing the dot file.
        """
        self._dot.render(path, view=True)


class Runner(process.Runner, key='graphviz'):
    """Graphviz based runner implementation.
    """
    FILEPATH = 'forml.dot'

    def __init__(self, assets: typing.Optional[access.Assets] = None, engine: typing.Optional[etl.Engine] = None,
                 filepath: typing.Optional[str] = None, **gvkw: typing.Any):
        super().__init__(assets, engine)
        self._filepath: str = filepath or self.FILEPATH
        self._gvkw: typing.Mapping[str, typing.Any] = gvkw

    def _run(self, symbols: typing.Sequence[code.Symbol]) -> grviz.Digraph:
        dot: grviz.Digraph = grviz.Digraph(**self._gvkw)
        for sym in symbols:
            attrs = dict(shape='ellipse', style='rounded')
            if isinstance(sym.instruction, instruction.Functor):
                attrs.update(shape='box')
            dot.node(str(id(sym.instruction)), str(sym.instruction), **attrs)
            for idx, arg in enumerate(sym.arguments):
                dot.edge(str(id(arg)), str(id(sym.instruction)), label=str(idx))
        dot.render(self._filepath, view=True)
        return dot
