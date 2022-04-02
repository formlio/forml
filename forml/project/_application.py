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
Product application utils.
"""
import abc
import collections
import pathlib
import typing

import forml
from forml.io import layout
from forml.runtime import asset

from . import _component


class Meta(abc.ABCMeta):
    """Descriptor metaclass."""

    def __call__(cls, *args, **kwargs):
        raise TypeError('Descriptor not instantiable')

    def __hash__(cls):
        return hash(cls.application)

    def __eq__(cls, other):
        return (
            isinstance(other, cls.__class__)
            and other.application == cls.application  # pylint: disable=comparison-with-callable
        )

    @property
    def application(cls) -> str:
        """Name of the application represented by this descriptor.

        Application name is expected to be globally unique.
        """
        return cls.__name__.lower()


class Descriptor(metaclass=Meta):
    """Application descriptor."""

    class Handle(collections.namedtuple('Handle', 'path, descriptor')):
        """Descriptor handle containing the descriptor instance and a filesystem path to a module containing it."""

        path: pathlib.Path
        """Filesystem path to the module containing the descriptor."""
        descriptor: type['Descriptor']
        """Actual descriptor."""

        def __new__(cls, path: typing.Union[str, pathlib.Path]):
            path = pathlib.Path(path).resolve()
            if not path.is_file():
                raise forml.InvalidError(f'Invalid descriptor module (file expected): {path}')

            descriptor = _component.load(path.with_suffix('').name, path.parent)
            if not issubclass(descriptor, Descriptor):
                raise forml.InvalidError(f'Invalid descriptor: {path}')

            return super().__new__(cls, path.resolve(), descriptor)

        def __getnewargs__(self):
            return tuple([self.path])

    @classmethod
    @abc.abstractmethod
    def decode(cls, request: layout.Request) -> layout.Request.Decoded:
        """Decode the raw payload into a format accepted by the application.

        Args:
            request: Native request format.

        Returns:
            Decoded entry with optional custom (serializable!) metadata to be carried over into ``select`` and
            ``encode``.
        """
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def encode(
        cls,
        outcome: layout.Outcome,
        encoding: typing.Sequence[layout.Encoding],
        scope: typing.Any,
    ) -> layout.Response:
        """Encode the application result into a native response to be passed back by the engine.

        Args:
            outcome: Output to be encoded.
            encoding: Accepted encoding media types.
            scope: Optional metadata carried over from decode.

        Returns:
            Encoded native response.
        """
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def select(
        cls,
        registry: asset.Directory,
        scope: typing.Any,
        stats: layout.Stats,
    ) -> asset.Instance:
        """Select the model instance to be used for serving the request.

        Args:
            registry: Model registry to select the model from.
            scope: Optional metadata carried over from decode.
            stats: Application specific serving metrics.

        Returns:
            Model instance.
        """
        raise NotImplementedError()
