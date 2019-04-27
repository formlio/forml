"""
Dataframe manipulation actors.

The implementation is not enforcing any consistency (in terms of number of inputs or their shapes etc).
"""
import logging
import typing

import pandas
from sklearn import model_selection

from forml.flow import task

LOGGER = logging.getLogger(__name__)


class TrainTestSplit(task.Actor):
    """Train-test splitter generation n-folds of train-test splits based on the provided crossvalidator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.
    """
    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self._crossvalidator: model_selection.BaseCrossValidator = crossvalidator
        self._indices: typing.Optional[typing.Sequence[typing.Tuple[typing.Sequence[int], typing.Sequence[int]]]] = None

    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the splitter on the provided data.
        Args:
            features: X table.
            label: Y series.
        """
        self._indices = self._crossvalidator.split(features, label)

    def apply(self, source: pandas.DataFrame) -> typing.Sequence[pandas.DataFrame]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            source: Input data set.

        Returns: Features with label column removed plus just the label column in second new dataset.
        """
        if not self._indices:
            raise RuntimeError('Splitter not trained')
        LOGGER.debug('Splitting %d rows into %d train-test sets', len(source), len(self._indices))
        return tuple(s for a, b in self._indices for s in (source[a], source[b]))

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {'crossvalidator': self._crossvalidator}

    def set_params(self,  # pylint: disable=arguments-differ
                   crossvalidator: model_selection.BaseCrossValidator) -> None:
        """Standard params setter.

        Args:
            crossvalidator: New crossvalidator.
        """
        self._crossvalidator = crossvalidator


class Append(task.Actor):
    """Vertically appending dataframes received on the input ports.
    """
    def apply(self, *tables: pandas.DataFrame) -> pandas.DataFrame:
        """Append the individual tables into one dataframe.

        Args:
            *tables: Individual dataframes to be appended.

        Returns: Single concatenated dataframe.
        """
        return pandas.concat(tables, axis='index', ignore_index=True)


class Merge(task.Actor):
    """Horizontally appending series received on the input ports.
    """

    def apply(self, *columns: pandas.Series) -> pandas.DataFrame:
        """Append the individual columns into one dataframe.

        Args:
            *columns: Individual columns of the dataframe.

        Returns: Single merged dataframe.
        """
        return pandas.concat(columns, axis='columns')


class Apply(task.Actor):
    """Generic dataframe apply actor.
    """
    def __init__(self, method: typing.Callable[[pandas.DataFrame], pandas.DataFrame]):
        self._method: typing.Callable[[pandas.DataFrame], pandas.DataFrame] = method

    def apply(self, table: pandas.DataFrame) -> pandas.DataFrame:  # pylint: disable=arguments-differ
        """Execute the provided method with the given table.

        Args:
            table: Dataframe to be passed through the provided method.

        Returns: Transformed output as returned by the provided method.
        """
        return self._method(table)
