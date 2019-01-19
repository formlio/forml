import typing

from forml import flow


class Project:
    """Top level ForML project descriptor.

    TODO: is explicit descriptor needed? this should be just discoverable based on the project layout!
    """
    def __init__(self):
        self._pipelines: typing.Sequence[flow.Pipeline] = ...
