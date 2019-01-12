import abc
import collections

import pandas

from forml.flow.graph import node


class Plan(collections.namedtuple('Plan', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """


class Pipeline(Operator):

    def train(self) -> 'Pipeline':
        """Train the pipeline.
        """

    def apply(self) -> pandas.DataFrame:
        """
        """
