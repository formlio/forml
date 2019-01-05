"""
Graph node entities.
"""
import typing

from forml.flow import task
from forml.flow.graph.layout import link


class Plain:
    """Single task graph node.
    """
    def __init__(self, actor: typing.Type[task.Actor], szin: int, szout: int):
        self.uid: str = ...
        self.actor = actor
        self.szin: int = szin
        # output ports:
        self.apply: typing.Tuple[link.Data] = tuple(link.Data() for _ in range(szout))
        self.state: link.State = link.State()

    @property
    def szout(self) -> int:
        """Width of the output apply port.

        Returns: Output apply port width.
        """
        return len(self.apply)
