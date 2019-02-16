"""
ForML top level.
"""

from forml import flow, etl
from forml.flow import segment


class Project:
    """Top level ForML project descriptor.

    """
    def __init__(self):
        self.pipeline: segment.Composable = ...
        self.source: etl.Source = ...
        self.scoring = ...  # cv+metric -> single number
        self.reporting = ...  # arbitrary metrics -> kv list
