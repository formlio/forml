from forml import flow


class Project:
    """Top level ForMLAI project descriptor.
    """
    def __init__(self):
        self._pipeline: flow.Pipeline = ...
