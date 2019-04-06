"""
Runtime that just renders the pipeline DAG visualization.
"""
import collections
import typing
import uuid

from dask.dot import graphviz

from forml.flow import task
from forml.flow.graph import view, node as grnode, port


class Dot(view.Visitor):
    """Path visitor for building the graphviz Dot structure.
    """
    def __init__(self, *args, **kwargs):
        self._dot: graphviz.Digraph = graphviz.Digraph(*args, **kwargs)
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
