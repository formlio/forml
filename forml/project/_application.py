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


class Descriptor(abc.ABC):
    """Application descriptor."""

    class Handle(collections.namedtuple('Handle', 'path, descriptor')):
        """Descriptor handle containing the descriptor instance and a filesystem path to a module containing it."""

        path: pathlib.Path
        """Filesystem path to the module containing the descriptor."""
        descriptor: 'Descriptor'
        """Actual descriptor instance."""

        def __new__(cls, path: typing.Union[str, pathlib.Path]):
            path = pathlib.Path(path).resolve()
            if not path.is_file():
                raise forml.InvalidError(f'Invalid descriptor module (plain file expected): {path}')

            descriptor = _component.load(path.with_suffix('').name, path.parent)
            if not isinstance(descriptor, Descriptor):
                raise forml.InvalidError('Invalid descriptor instance')

            return super().__new__(cls, path.resolve(), descriptor)

        def __getnewargs__(self):
            return tuple([self.path])

    def serve(self, request: layout.Request, registry: asset.Directory) -> layout.Response:
        entry = self.decode(request)
        instance = self.select(registry, entry)

    @classmethod
    @abc.abstractmethod
    def decode(cls, request: layout.Request) -> layout.Entry:
        """Decode the raw payload into a format accepted by the application.

        Args:
            request: Native request format.

        Returns:
            Decoded entry.
        """
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def encode(
        cls, result: layout.Result, entry: layout.Entry, encoding: typing.Sequence[layout.Encoding]
    ) -> layout.Response:
        """Encode the application result into a native response to be passed back by the engine.

        Args:
            result: Output to be encoded.
            entry: Decoded input query entry.
            encoding: Accepted encoding media types.

        Returns:
            Encoded native response.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def select(self, registry: asset.Directory, entry: layout.Entry, **kwargs) -> asset.Instance:
        """Select the model instance to be used for serving the request.

        Args:
            registry: Model registry to select the model from.
            entry: Decoded input query entry.

        Returns:
            Model instance.
        """
        raise NotImplementedError()
