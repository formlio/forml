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
from forml.flow.graph import view, node as grnode


class NodeValidator(view.Visitor):
    """Visitor ensuring all nodes are in valid state which means:

        * are Worker instances (not Future)
        * have consistent subscriptions:
            * only both train and label or all input ports
            * only both train and label or all output ports
    """

    def visit_node(self, node: grnode.Atomic) -> None:
        """Node visit.

        Args:
            node: Node to be visited.
        """
