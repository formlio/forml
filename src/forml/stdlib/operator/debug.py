"""
Debugging operators.
"""
import abc
import os
import secrets
import typing

import pandas

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
        inserter: node.Worker = node.Worker(self.inserter, 1, 1)
        inserter.train(left.train.publisher, left.label.publisher)
        return left.extend(train=view.Path(inserter.fork()))


class Dumper(task.Actor, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Pass-through transformer that dumps the input datasets to CSV files.
    """
    def __init__(self, path: str):
        self.path: str = path

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {'path': self.path}

    def set_params(self, path: str) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            path: New path.
        """
        self.path = path


class ApplyDumper(Dumper):
    """Pass-through transformer that dumps the input datasets during apply phase to CSV files.
    """
    @frame.ndframed
    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:  # pylint: disable=arguments-differ
        """Dump the features.

        Args:
            features: Input frames.

        Returns: Original unchanged frames.
        """
        features.to_csv(self.path, index=False)
        return features


class TrainDumper(Dumper):
    """Pass-through transformer that dumps the input datasets during train phase to CSV files.
    """

    def __init__(self, path: str, label: str = 'label'):
        super().__init__(path)
        self.label: str = label

    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:  # pylint: disable=arguments-differ
        """No-op transformation.

        Args:
            features: Input frames.

        Returns: Original unchanged frames.
        """
        return features

    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Dump the features along with labels.

        Args:
            features: X table.
            label: Y series.
        """
        features.set_index(label.rename(self.label)).reset_index().to_csv(self.path, index=False)

    def get_state(self) -> bytes:
        """We aren't really stateful.

        Return: Empty state.
        """
        return bytes()

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {**super().get_params(), 'label': self.label}

    def set_params(self, label: typing.Optional[str] = None, **params: str) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            label: New label.
        """
        if label:
            self.label = label
        super().set_params(**params)


class Dump(topology.Operator):
    """Transparent transformer that dumps the input datasets to CSV files.
    """
    CSV_SUFFIX = '.csv'

    def __init__(self, path: str = '', label: str = 'label'):
        self.dir: str = os.path.dirname(path)
        name, suffix = os.path.splitext(os.path.basename(path))
        self.name: str = name or secrets.token_urlsafe(8)
        self.suffix: str = suffix or self.CSV_SUFFIX
        self.label: str = label
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
        train_dumper: node.Worker = node.Worker(TrainDumper.spec(path=self._path('train'), label=self.label), 1, 1)
        apply_dumper: node.Worker = node.Worker(ApplyDumper.spec(path=self._path('apply')), 1, 1)
        train_dumper.train(left.train.publisher, left.label.publisher)
        self._instances += 1
        return left.extend(apply=view.Path(apply_dumper), train=view.Path(train_dumper.fork()))
