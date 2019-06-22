"""
Debugging operators.
"""

import os
import secrets

from forml.flow import task, pipeline
from forml.flow.graph import node, view
from forml.flow.pipeline import topology
from forml.stdlib.actor import label as labelmod, frame


class Return(topology.Operator):
    """Transformer that for train flow re-inserts label back to the frame and returns it (apply flow remains unchanged).
    This is useful for cutting a pipeline and appending this operator to return the dataset as is for debugging.
    """
    def __init__(self, label: str = 'label'):
        self.inserter: task.Spec = labelmod.ColumnInserter.spec(column=label)

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Composition implementation.

        Args:
            left: Left side.

        Returns: Composed track.
        """
        left: pipeline.Segment = left.expand()
        inserter_apply: node.Worker = node.Worker(self.inserter, 1, 1)
        inserter_train: node.Worker = inserter_apply.fork()
        inserter_train.train(left.train.publisher, left.label.publisher)
        return left.extend(train=view.Path(inserter_apply))


class Dump(topology.Operator):
    """Transparent transformer that dumps the input datasets to CSV files.
    """
    CSV_SUFFIX = '.csv'

    def __init__(self, path: str = '', label: str = 'label'):
        self.dir: str = os.path.dirname(path)
        name, suffix = os.path.splitext(os.path.basename(path))
        self.name: str = name or secrets.token_urlsafe(8)
        self.suffix: str = suffix or self.CSV_SUFFIX
        self.inserter: task.Spec = labelmod.ColumnInserter.spec(column=label)
        self._instances: int = 0

    def _path(self, mode: str) -> str:
        """Generate the target path.

        Args:
            mode: Pipeline operation mode.

        Returns: Path value.
        """
        return os.path.join(self.dir, f'{self.name}-{mode}-{self._instances}{self.suffix}')

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Composition implementation.

        Args:
            left: Left side.

        Returns: Composed track.
        """
        left: pipeline.Segment = left.expand()
        inserter_apply: node.Worker = node.Worker(self.inserter, 1, 1)
        inserter_train: node.Worker = inserter_apply.fork()
        inserter_train.train(left.train.publisher, left.label.publisher)
        dumper_train: node.Worker = node.Worker(frame.Dump.spec(path=self._path('train')), 1, 1)
        dumper_apply: node.Worker = node.Worker(frame.Dump.spec(path=self._path('apply')), 1, 1)
        dumper_train[0].subscribe(inserter_apply[0])
        self._instances += 1
        return left.extend(apply=view.Path(dumper_apply), train=view.Path(inserter_apply, dumper_train))
