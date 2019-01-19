import typing

from forml.flow.graph import node, port


# chain
# channel
# closure

class Compound:
    """Node representing compound acyclic flow - a sub-graph with single head and tail node each with at most one
    apply input/output port.
    """

    def __init__(self, head: node.Atomic):
        def tailof(publisher: node.Atomic, path: typing.FrozenSet[node.Atomic] = frozenset()) -> node.Atomic:
            """Recursive traversing all apply subscription paths up to the tail checking there is just one.

            Args:
                publisher: Start node for the traversal.
                path: Chain of nodes between current and head.

            Returns: Head of the flow.
            """
            subscribers = set(s.node for p in publisher.output for s in p if isinstance(s.port, port.Apply))
            if not any(subscribers):
                return publisher
            path = frozenset(path | {publisher})
            assert subscribers.isdisjoint(path), 'Cyclic flow not condensable'
            tails = set(tailof(n, path) for n in subscribers)
            assert len(tails) == 1, 'Open flow not condensable'
            return tails.pop()

        assert head.szin in {0, 1}, 'Head node not condensable'
        tail: node.Atomic = tailof(head)
        assert tail.szout in {0, 1}, 'Tail node not condensable'
        self._head: node.Atomic = head
        self._tail: node.Atomic = tail

    def extend(self, right: 'Compound') -> None:
        """Extend this compound by appending right head to our tail.

        Args:
            right: Compound node to extend with.
        """
        right._head[0].subscribe(self._tail[0])
        self._tail = right._tail

    @property
    def subscriber(self) -> port.Subscriptable:
        """Subscriptable head node representation.

        Returns: Subscriptable head apply port reference.
        """
        return self._head[0].subscriber

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """
        return self._tail[0].publisher

    def copy(self) -> 'Compound':
        """Make a copy of the compound topology which must not contain any trained nodes.

        Returns: Copy of the compound sub-graph.
        """
        def copyof(publisher: node.Atomic) -> node.Atomic:
            """Recursive copy resolver.

            Args:
                publisher: Node to be copied.

            Returns: Copy of the publisher node with all of it's subscriptions resolved.
            """
            if publisher not in mapping:
                copied = publisher.copy()
                if publisher is not self._tail:
                    for index, subscription in ((i, s) for i, p in enumerate(publisher.output) for s in p):
                        subscriber = mapping.get(subscription.node) or copyof(subscription.node)
                        copied[index].publish(subscriber, subscription.port)
                mapping[publisher] = copied
            return mapping[publisher]

        mapping = dict()
        return super().__new__(self.__class__, copyof(self._head), copyof(self._tail))
