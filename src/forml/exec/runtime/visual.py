"""
Runtime that just renders the pipeline DAG visualization.
"""

from dask.dot import graphviz

from forml.flow.graph import view, node as grnode, port


class Dot(view.PreOrder):
    """Path visitor for building the graphviz Dot structure.
    """
    def __init__(self, *args, **kwargs):
        self._dot: graphviz.Digraph = graphviz.Digraph(*args, **kwargs)

    def visit_node(self, node: grnode.Atomic) -> None:
        """Process new node.

        Args:
            node: Node to be processed.
        """
        self._dot.node(str(id(node)), str(node))
        for index, subscription in ((i, s) for i, p in enumerate(node.output) for s in p):
            self._dot.edge(str(id(node)), str(id(subscription.node)), label=f'{port.Apply(index)}->{subscription.port}')

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
