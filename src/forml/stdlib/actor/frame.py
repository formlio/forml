"""
Dataframe manipulation actors.

The implementation is not enforcing any consistency (in terms of number of inputs or their shapes etc).
"""
import functools
import logging
import typing

import numpy
import pandas
from pandas.core import generic as pdtype
from sklearn import model_selection

from forml.flow import task

LOGGER = logging.getLogger(__name__)


def ndframed(wrapped: typing.Callable[[task.Actor, pdtype.NDFrame],
                                      typing.Any]) -> typing.Callable[[task.Actor, typing.Any], typing.Any]:
    """Decorator for converting input parameters and return value to pandas.

    Args:
        wrapped: Actor method to be decorated.

    Returns: Decorated method.
    """
    def convert(arg: typing.Any) -> pdtype.NDFrame:
        """Conversion logic.

        Args:
            arg: Argument to be converted.

        Returns: Converted pandas object.
        """
        if isinstance(arg, pdtype.NDFrame):
            return arg
        if isinstance(arg, numpy.ndarray):
            return pandas.Series(arg) if arg.ndim == 1 else pandas.DataFrame(arg)
        LOGGER.warning('Unknown NDFrame conversion strategy for %s', type(arg))
        return arg

    @functools.wraps(wrapped)
    def wrapper(self: task.Actor, *args: typing.Any) -> pdtype.NDFrame:
        """Decorating wrapper.

        Args:
            self: Actor self.
            *args: Input arguments to be converted.

        Returns: Converted output of original method.
        """
        return convert(wrapped(self, *(convert(a) for a in args)))
    return wrapper


class TrainTestSplit(task.Actor):
    """Train-test splitter generation n-folds of train-test splits based on the provided crossvalidator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.
    """
    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self.crossvalidator: model_selection.BaseCrossValidator = crossvalidator
        self._indices: typing.Optional[typing.Tuple[typing.Tuple[typing.Sequence[int], typing.Sequence[int]]]] = None

    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the splitter on the provided data.
        Args:
            features: X table.
            label: Y series.
        """
        self._indices = tuple(self.crossvalidator.split(features, label))  # tuple it so it can be pickled

    @ndframed
    def apply(self, source: pandas.DataFrame) -> typing.Sequence[pandas.DataFrame]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            source: Input data set.

        Returns: Features with label column removed plus just the label column in second new dataset.
        """
        if not self._indices:
            raise RuntimeError('Splitter not trained')
        LOGGER.debug('Splitting %d rows into %d train-test sets', len(source), len(self._indices))
        return tuple(s for a, b in self._indices for s in (source.iloc[a], source.iloc[b]))

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Standard param getter.

        Returns: Actor params.
        """
        return {'crossvalidator': self.crossvalidator}

    def set_params(self,  # pylint: disable=arguments-differ
                   crossvalidator: model_selection.BaseCrossValidator) -> None:
        """Standard params setter.

        Args:
            crossvalidator: New crossvalidator.
        """
        self.crossvalidator = crossvalidator


class Concat(task.Actor):
    """Concatenate objects received on the input ports into single dataframe.
    """
    def __init__(self, axis: str = 'index'):
        self.axis: str = axis

    @ndframed
    def apply(self, *source: pdtype.NDFrame) -> pandas.DataFrame:
        """Concat the individual objects into one dataframe.

        Args:
            *source: Individual sources to be concatenated.

        Returns: Single concatenated dataframe.
        """
        return pandas.concat(source, axis=self.axis, ignore_index=True)


class Apply(task.Actor):
    """Generic source apply actor.
    """
    def __init__(self, function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame]):
        self.function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame] = function

    @ndframed
    def apply(self, *source: pdtype.NDFrame) -> pdtype.NDFrame:  # pylint: disable=arguments-differ
        """Execute the provided method with the given sources.

        Args:
            source: Inputs to be passed through the provided method.

        Returns: Transformed output as returned by the provided method.
        """
        return self.function(*source)
