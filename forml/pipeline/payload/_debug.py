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
import logging
import pathlib
import secrets
import socket
import string
import typing
from multiprocessing import managers

from pandas.core import generic as pdtype

import forml
from forml import flow

from . import _convert

if typing.TYPE_CHECKING:
    from forml.pipeline import payload  # nopycln: import


LOGGER = logging.getLogger(__name__)


class Dumpable(
    flow.Actor[flow.Features, flow.Labels, flow.Result], metaclass=abc.ABCMeta
):  # pylint: disable=abstract-method
    """Transparent actor interface that dumps the input dataset (typically to a file).

    Args:
        path: Target path to be used for dumping the content.
        kwargs: Optional keyword arguments to be passed to the :meth:`apply_dump` and/or
                  :meth:`train_dump` methods.

    Following are the methods that can be overloaded with the actual dump action (no-op otherwise).

    Methods:

        apply_dump(features, path, **kwargs):
            Dump the features when in the *apply-mode*.

            Args:
                features: Input data.
                path: Target dump location.
                kwargs: Additional keyword arguments supplied via constructor.

        train_dump(features, labels, path, **kwargs):
            Dump the features and labels when in the *train-mode*.

            Args:
                features: Input features.
                labels: Input labels.
                path: Target dump location.
                kwargs: Additional keyword arguments supplied via constructor.
    """

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
            kwargs: Additional keyword arguments supplied via constructor.
        """
        return None

    def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
        """Standard actor train method calling the dump.

        Args:
            features: Input features.
            labels: Input labels.
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
            features: Input features.
            labels: Input labels.
            path: Target dump location.
            kwargs: Additional keyword arguments supplied via constructor.
        """
        return None

    @classmethod
    def is_stateful(cls) -> bool:
        return cls.train_dump.__code__ is not Dumpable.train_dump.__code__

    @typing.final
    def get_state(self) -> None:
        """We aren't really stateful even though `.is_stateful()` can be true so that `.train()`
        get engaged.

        Return: Empty state.
        """
        return None

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


class PandasCSVDumper(Dumpable[typing.Any, typing.Any, typing.Any]):
    """PandasCSVDumper(path: typing.Union[str, pathlib.Path], label_header: str = 'Label', converter: typing.Callable[[typing.Any, typing.Optional[typing.Sequence[str]]], pandas.core.generic.NDFrame] = pandas_read, **kwargs)

    A pass-through transformer that dumps the input datasets to CSV files.

    The write operation including the CSV encoding is implemented using the
    :meth:`pandas:pandas.DataFrame.to_csv` method.

    The input payload is automatically converted to Pandas using the provided converter
    implementation (defaults to internal logic).

    Args:
        path: Target path to be used for dumping the content.
        label_header: Column name to be used for the train labels.
        converter: Optional callback to be used for converting the payload to Pandas.
        kwargs: Optional keyword arguments to be passed to the
                :meth:`pandas:pandas.DataFrame.to_csv` method.
    """  # pylint: disable=line-too-long  # noqa: E501

    CSV_DEFAULTS = {'index': False}

    def __init__(
        self,
        path: typing.Union[str, pathlib.Path],
        label_header: str = 'Label',
        converter: typing.Callable[
            [typing.Any, typing.Optional[typing.Sequence[str]]], pdtype.NDFrame
        ] = _convert.pandas_read,
        **kwargs,
    ):
        super().__init__(path, label_header=label_header, converter=converter, **kwargs)

    @classmethod
    def apply_dump(
        cls,
        features: typing.Any,
        path: pathlib.Path,
        label_header: str,  # pylint: disable=unused-argument
        converter: typing.Callable[[typing.Any, typing.Optional[typing.Sequence[str]]], pdtype.NDFrame],
        **kwargs,
    ) -> None:
        """Dump the features.

        Args:
            features: Input frames.
            path: Target dump location.
            label_header: Column name to be used for the train labels.
            converter: Pandas converter implementation.
            kwargs: Additional keyword arguments supplied via constructor.

        Returns:
            Original unchanged frames.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        converter(features).to_csv(path, **(cls.CSV_DEFAULTS | kwargs))

    @classmethod
    def train_dump(
        cls,
        features: typing.Any,
        labels: typing.Any,
        path: pathlib.Path,
        label_header: str,
        converter: typing.Callable[[typing.Any, typing.Optional[typing.Sequence[str]]], pdtype.NDFrame],
        **kwargs,
    ) -> None:
        """Dump the features along with labels.

        Args:
            features: X table.
            labels: Y series.
            path: Target dump location.
            label_header: Column name to be used for the train labels.
            converter: Pandas converter implementation.
            kwargs: Additional keyword arguments supplied via constructor.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        converter(features).set_index(converter(labels).rename(label_header)).reset_index().to_csv(
            path, **(cls.CSV_DEFAULTS | kwargs)
        )


class Dump(flow.Operator):
    """Dump(apply: flow.Builder[payload.Dumpable] = PandasCSVDumper.builder(), train: typing.Optional[flow.Builder[payload.Dumpable]] = None, *, path: typing.Optional[typing.Union[str, pathlib.Path]] = None)

    A transparent operator that dumps the input dataset externally (typically to a file) before
    passing it downstream.

    If supplied as a template, the operator supports interpolation of potential placeholders in
    the dump path (e.g. file name). The supported placeholders are:

    * ``$seq`` - a sequence ID that gets incremented for each particular Actor instance
    * ``$mode`` - a label of the mode in which the dumping occurs (``train`` or ``apply``)

    The path (or path template) must be provided either within the raw builder parameters or as the
    standalone ``path`` parameter which is used as a fallback option.

    Args:
        apply: ``Dumpable`` builder for instantiating a Dumper operator to be used in *apply-mode*.
        train: Optional ``Dumpable`` builder for instantiating a Dumper operator to be used in
               *train-mode* (otherwise using the same one as for the *apply-mode*).
        path: Optional path template.

    Raises:
        TypeError: If *path* is provided neither in the ``apply``/``train`` builders nor as
                   the explicit parameter.

    Examples:
        >>> PIPELINE = (
        ...     preprocessing.Action1()
        ...     >> payload.Dump(path='/tmp/foobar/post_action1-$mode-$seq.csv')
        ...     >> preprocessing.Action2()
        ...     >> payload.Dump(path='/tmp/foobar/post_action2-$mode-$seq.csv')
        ...     >> model.SomeModel()
        ... )
    """  # pylint: disable=line-too-long  # noqa: E501

    CSV_SUFFIX = '.csv'

    def __init__(
        self,
        apply: flow.Builder['payload.Dumpable'] = PandasCSVDumper.builder(),  # noqa: B008
        train: typing.Optional[flow.Builder['payload.Dumpable']] = None,
        *,
        path: typing.Optional[typing.Union[str, pathlib.Path]] = None,
    ):
        self._apply: typing.Callable[[int], flow.Builder['payload.Dumpable']] = self._meta_builder(apply, path, 'apply')
        self._train: typing.Callable[[int], flow.Builder['payload.Dumpable']] = self._meta_builder(
            train or apply, path, 'train'
        )
        self._instances: int = 0

    @classmethod
    def _meta_builder(
        cls,
        builder: flow.Builder['payload.Dumpable'],
        path: typing.Optional[typing.Union[str, pathlib.Path]],
        mode: str,
    ) -> typing.Callable[[int], flow.Builder['payload.Dumpable']]:
        """Get a function for creating a Builder instance parameterized using a sequence id and
        a mode string which can be used to interpolate potential placeholders in the path template.

        Args:
            builder: Raw builder to be enhanced. Optional path parameter can have template
                     placeholders.
            path: Optional path with potential template placeholders.

        Returns:
            Factory function for creating a builder instance with the potential path template
            placeholders interpolated.
        """

        def wrapper(seq: int) -> flow.Builder['payload.Dumpable']:
            """The factory function for creating a builder instance with path template
            interpolation.
            """

            interpolated = string.Template(str(pathlib.Path(path))).safe_substitute(seq=seq, mode=mode)
            return builder.update(path=interpolated, **binding.kwargs)

        binding = inspect.signature(builder.actor).bind_partial(*builder.args, **builder.kwargs)
        binding.apply_defaults()
        path = binding.arguments.pop('path', path)
        if not path:
            raise TypeError('Path is required')
        return wrapper

    def compose(self, scope: flow.Composable) -> flow.Trunk:
        """Composition implementation.

        Args:
            scope: Left side.

        Returns:
            Composed track.
        """
        left: flow.Trunk = scope.expand()
        apply: flow.Worker = flow.Worker(self._apply(self._instances), 1, 1)
        train: flow.Worker = flow.Worker(self._train(self._instances), 1, 1)
        train.train(left.train.publisher, left.label.publisher)
        self._instances += 1
        return left.extend(apply=apply)


class Sniff(flow.Operator):
    """Debugging operator for capturing the passing payload and exposing it using the
    ``Sniff.Value.Future`` instance provided when used as a context manager.

    Without the context, the operator acts as a transparent identity pass-through operator.

    The typical use case is in combination with the :class:`runtime.virtual <forml.runtime.Virtual>`
    launcher and the :ref:`interactive mode <interactive>`.

    Examples:
        >>> SNIFFER = payload.Sniff()
        >>> with SNIFFER as future:
        ...     SOURCE.bind(PIPELINE >> SNIFFER >> ANOTHER).launcher.train()
        >>> future.result()[0]
    """

    class Captor(flow.Actor[flow.Features, flow.Labels, flow.Result]):
        """Actor for sniffing all inputs and passing it to the remote value."""

        def __init__(self, value: typing.Optional['Sniff.Value.Client']):
            self._value: typing.Optional['Sniff.Value.Client'] = value

        def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
            if self._value is not None:
                self._value.set((features, labels))

        def apply(self, features: flow.Features) -> flow.Result:
            if self._value is not None:
                self._value.set(features)
            return features

        def get_state(self) -> None:
            """Not really having any state."""
            return None

    class Lost(forml.MissingError):
        """Custom error indicating absence of the result."""

    class Value:
        """Remote value delivering facility."""

        class Future:
            """Future object to eventually contain the sniffed value."""

            def __init__(self):
                self._value: typing.Any = None
                self._empty: bool = False
                self._done: bool = False

            def result(self) -> typing.Any:
                """Get the future result.

                Returns:
                    Future result.

                Raises:
                    payload.Sniff.Lost: If the sniffer didn't capture anything.
                """
                if not self._done:
                    raise RuntimeError('Future still pending')
                if self._empty:
                    raise Sniff.Lost('Sniffer value empty')
                return self._value

            def set_result(self, value: typing.Any) -> None:
                """Set the future result.

                Args:
                    value: Future result value.
                """
                assert not self._done
                self._value = value
                self._done = True

            def set_empty(self) -> None:
                """Set the result to indicate absence of any value data."""
                assert not self._done
                self._empty = True
                self._done = True

        class Client:
            """Remote value client."""

            def __init__(self, manager: type[managers.BaseManager], address: tuple[str, int], authkey: bytes):
                self._manager: type[managers.BaseManager] = manager
                self._address = address
                self._authkey = authkey

            @property
            def _value(self) -> managers.ValueProxy:
                """Create and connect the manager and return its value proxy."""
                manager = self._manager(address=self._address, authkey=self._authkey)
                manager.connect()
                return manager.value()

            def set(self, value: typing.Any) -> None:
                """Set the remote value."""
                self._value.set(value)

        def __init__(self, manager: managers.BaseManager, result: 'Sniff.Value.Future', empty: typing.Any):
            self._manager: managers.BaseManager = manager
            self._result: Sniff.Value.Future = result
            self._empty: typing.Any = empty

        @classmethod
        def open(cls) -> tuple['Sniff.Value', 'Sniff.Value.Client', 'Sniff.Value.Future']:
            """Create new remote value facility.

            Returns:
                Tuple of the remote value instance, its client and the actual result future.
            """

            class Manager(managers.BaseManager):
                """Custom manager type."""

            empty = object()
            value = managers.Value(object, empty)
            Manager.register('value', callable=lambda: value)
            authkey = secrets.token_bytes(16)
            manager = Manager((socket.gethostname(), 0), authkey=authkey)
            manager.start()  # pylint: disable=consider-using-with
            client = cls.Client(Manager, manager.address, authkey=authkey)
            result = cls.Future()
            return cls(manager, result, empty), client, result

        def close(self) -> None:
            """Collect the result and close the remote value manager."""
            value = self._manager.value().get()
            if value is self._empty:
                self._result.set_empty()
            else:
                self._result.set_result(value)
            self._manager.shutdown()

    def __init__(self):
        self._value: typing.Optional['Sniff.Value'] = None
        self._client: typing.Optional['Sniff.Value.Client'] = None

    def __enter__(self) -> 'payload.Sniff.Value.Future':
        assert self._value is None
        self._value, self._client, result = self.Value.open()
        return result

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._value.close()  # pylint: disable=no-member
        self._value = self._client = None

    def compose(self, scope: flow.Composable) -> flow.Trunk:
        left = scope.expand()
        apply = flow.Worker(self.Captor.builder(self._client), 1, 1)
        train = flow.Worker(self.Captor.builder(self._client), 1, 1)
        train.train(left.train.publisher, left.label.publisher)
        return left.extend(apply=apply)
