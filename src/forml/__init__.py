"""
ForML top level.
"""

from forml import flow
from forml.flow import segment


class Project:
    """Top level ForML project descriptor.

    """
    def __init__(self):
        self.pipeline: segment.Composable = ...
        self.source: segment.Composable = ...
        self.scoring = ...  # cv+metric -> single number
        self.reporting = ...  # arbitrary metrics -> kv list
