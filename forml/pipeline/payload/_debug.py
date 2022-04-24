# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Debugging operators.
"""
import abc
import inspect
import pathlib
import string
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow

from . import _convert, _generic

if typing.TYPE_CHECKING:
    from forml.pipeline import payload


class Dumpable(
    flow.Actor[flow.Features, flow.Labels, flow.Result], metaclass=abc.ABCMeta
):  # pylint: disable=abstract-method
    """Pass-through abstract transformer that dumps the input datasets to CSV files."""

    def __init__(self, path: typing.Union[str, pathlib.Path], **kwargs):
        self._path: pathlib.Path = pathlib.Path(path)
        self._kwargs: dict[str, typing.Any] = kwargs

    def apply(self, features: flow.Features) -> flow.Result:  # pylint: disable=arguments-differ
        """Standard actor apply method calling the dump.

        Args:
            features: Input frames.

        Returns:
            Original unchanged frames.
        """
        self.apply_dump(features, self._path, **self._kwargs)
        return features

    @classmethod
    def apply_dump(
        cls, features: flow.Features, path: pathlib.Path, **kwargs  # pylint: disable=unused-argument
    ) -> None:
        """Dump the features.

        Args:
            features: Input data.
            path: Target dump location.
        """
        return None

    def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
        """Standard actor train method calling the dump.

        Args:
            features: X table.
            labels: Y series.
        """
        self.train_dump(features, labels, self._path, **self._kwargs)

    @classmethod
    def train_dump(
        cls,
        # pylint: disable=unused-argument
        features: flow.Features,
        labels: flow.Labels,
        path: pathlib.Path,
        **kwargs,
    ) -> None:
        """Dump the features along with labels.

        Args:
            features: X table.
            labels: Y series.
            path: Target dump location.
        """
        return None

    @typing.final
    def get_state(self) -> bytes:
        """We aren't really stateful even though `.is_stateful()` must be true so that `.train()` get engaged.

        Return: Empty state.
        """
        return bytes()

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return {'path': self._path, **self._kwargs}

    def set_params(self, path: str, **kwargs) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            path: New path.
            kwargs: Dumper kwargs.
        """
        self._path = path
        self._kwargs.update(kwargs)


class PandasApplyCSVDumper(Dumpable[pdtype.NDFrame, None, pdtype.NDFrame]):
    """Pass-through transformer that dumps the input datasets during apply phase to CSV files."""

    def __init__(self, path: typing.Union[str, pathlib.Path]):  # pylint: disable=useless-super-delegation
        super().__init__(path)

    @_convert.pandas_params
    def apply(self, features: pdtype.NDFrame) -> pdtype.NDFrame:
        return super().apply(features)

    @classmethod
    def apply_dump(cls, features: pdtype.NDFrame, path: pathlib.Path, **kwargs) -> None:
        """Dump the features.

        Args:
            features: Input frames.
            path: Target dump location.

        Returns:
            Original unchanged frames.
        """
        features.to_csv(path, index=False)


class PandasTrainCSVDumper(Dumpable[pandas.DataFrame, pandas.Series, pandas.DataFrame]):
    """Pass-through transformer that dumps the input datasets during train phase to CSV files."""

    def __init__(self, path: typing.Union[str, pathlib.Path], label_header: str = 'Label'):
        super().__init__(path, label_header=label_header)

    @_convert.pandas_params
    def train(self, features: pandas.DataFrame, labels: pandas.Series, /) -> None:
        super().train(features, labels)

    @classmethod
    def train_dump(cls, features: pandas.DataFrame, labels: pandas.Series, path: pathlib.Path, **kwargs) -> None:
        """Dump the features along with labels.

        Args:
            features: X table.
            labels: Y series.
            path: Target dump location.
        """
        features.set_index(labels.rename(kwargs['label_header'])).reset_index().to_csv(path, index=False)


class Dump(flow.Operator):
    """Transparent transformer that dumps the input datasets to CSV files.

    The operator supports interpolation of potential placeholders in the output path if supplied as template. The
    supported placeholders are:

        * `$seq` - a sequence ID which gets incremented for each particular Actor instance
        * `$mode` - a label of the mode in which the dumping occurs (`train` or `apply`)

    The path (or path template) must be provided either within the raw spec parameters or as the standalone `path`
    parameter which is used as a fallback option.
    """

    CSV_SUFFIX = '.csv'

    def __init__(
        self,
        apply: flow.Spec['payload.Dumpable'] = PandasApplyCSVDumper.spec(),  # noqa: B008
        train: typing.Optional[flow.Spec['payload.Dumpable']] = PandasTrainCSVDumper.spec(),  # noqa: B008
        *,
        path: typing.Optional[str] = None,
    ):
        self._apply: typing.Callable[[int], flow.Spec['payload.Dumpable']] = self._spec_builder(apply, path, 'apply')
        self._train: typing.Callable[[int], flow.Spec['payload.Dumpable']] = self._spec_builder(
            train or apply, path, 'train'
        )
        self._instances: int = 0

    @classmethod
    def _spec_builder(
        cls, spec: flow.Spec['payload.Dumpable'], path: typing.Optional[str], mode: str
    ) -> typing.Callable[[int], flow.Spec['payload.Dumpable']]:
        """Get a function for creating a Spec instance parametrized using a sequence id and a mode string which can be
        used to interpolate potential placeholders in the path template.

        Args:
            spec: Raw spec to be enhanced. Optional path parameter can have template placeholders.
            path: Optional path with potential template placeholders.

        Returns:
            Factory function for creating a spec instance with the potential path template placeholders interpolated.
        """

        def mkspec(seq: int) -> flow.Spec['payload.Dumpable']:
            """The factory function for creating a spec instance with path template interpolation."""

            interpolated = string.Template(str(pathlib.Path(path))).safe_substitute(seq=seq, mode=mode)
            return spec.actor.spec(*binding.args, path=interpolated, **binding.kwargs)

        binding = inspect.signature(spec.actor).bind_partial(*spec.args, **spec.kwargs)
        binding.apply_defaults()
        path = binding.arguments.pop('path', path)
        if not path:
            raise TypeError('Path is required')
        return mkspec

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Composition implementation.

        Args:
            left: Left side.

        Returns:
            Composed track.
        """
        left: flow.Trunk = left.expand()
        train_dumper: flow.Worker = flow.Worker(self._train(self._instances), 1, 1)
        apply_dumper: flow.Worker = flow.Worker(self._apply(self._instances), 1, 1)
        train_dumper.train(left.train.publisher, left.label.publisher)
        self._instances += 1
        return left.extend(apply=flow.Path(apply_dumper), train=flow.Path(train_dumper.fork()))


class Return(flow.Operator):
    """Transformer that for train flow merges labels back to the frame and returns it (apply flow remains unchanged).
    This is useful for cutting a pipeline and appending this operator to return the dataset as is for debugging.
    """

    def __init__(
        self,
        label_merger: flow.Spec['payload.LabelMergeable'] = _generic.PandasLabelMerger.spec(),  # noqa: B008
    ):
        self._label_merger: 'flow.Spec[payload.LabelMergeable]' = label_merger

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Composition implementation.

        Args:
            left: Left side.

        Returns:
            Composed track.
        """
        left: flow.Trunk = left.expand()
        label_merger: flow.Worker = flow.Worker(self._label_merger, 1, 1)
        label_merger.train(left.train.publisher, left.label.publisher)
        return left.extend(train=flow.Path(label_merger.fork()))