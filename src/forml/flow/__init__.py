import pandas

from forml.flow import graph


class Pipeline(graph.Operator):

    def train(self) -> 'Pipeline':
        """Train the pipeline.
        """

    def apply(self) -> pandas.DataFrame:
        """
        """
