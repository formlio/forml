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
import inspect
import pathlib
import typing

import forml
from forml.project import _component

if typing.TYPE_CHECKING:
    from forml.io import asset, layout


class Descriptor(abc.ABC):
    """Application descriptor abstract base class.

    The *serving layer* is using Application descriptors to drive the request processing.

    Descriptors are managed through :class:`asset.Inventory <forml.io.asset.Inventory>`.
    """

    class Handle(collections.namedtuple('Handle', 'path, descriptor')):
        """Descriptor handle containing the descriptor instance and a filesystem path to a module
        containing it.
        """

        path: pathlib.Path
        """Filesystem path to the module containing the descriptor."""
        descriptor: 'Descriptor'
        """Actual descriptor."""

        def __new__(cls, path: typing.Union[str, pathlib.Path]):
            path = pathlib.Path(path).resolve()
            if not path.is_file():
                raise forml.InvalidError(f'Invalid descriptor module (file expected): {path}')
            module = inspect.getmodulename(path)
            if not module:
                raise forml.InvalidError(f'Invalid descriptor module (not a module): {path}')
            descriptor = _component.load(module, path.parent)
            if not descriptor:
                raise forml.InvalidError(f'Invalid descriptor (no setup): {path}')
            if not isinstance(descriptor, Descriptor):
                raise forml.InvalidError(f'Invalid descriptor (wrong type): {path}')

            return super().__new__(cls, path.resolve(), descriptor)

        def __getnewargs__(self):
            return tuple([self.path])

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, Descriptor) and other.name == self.name

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Name of the application represented by this descriptor.

        Caution:
            Application name is expected to be globally unique.

        Returns:
            Application name.
        """

    @abc.abstractmethod
    def receive(self, request: 'layout.Request') -> 'layout.Request.Decoded':
        """Receive the raw payload and turn it into a structure suitable for predicting.

        This involves at least message decoding plus potentially also any adjustments to the data
        necessary for prediction. Additionally, it might also produce custom metadata representing
        an *application scope* to be passed down the chain all the way to :meth:`select` and
        :meth:`encode`.

        Args:
            request: Native request format.

        Returns:
            Decoded entry (adjusted for prediction) with optional custom (serializable!) metadata.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def select(
        self,
        registry: 'asset.Directory',
        scope: typing.Any,
        stats: 'layout.Stats',
    ) -> 'asset.Instance':
        """Select the model instance to be used for serving the request.

        Args:
            registry: Model registry to select the model from.
            scope: Optional metadata carried over from :meth:`receive`.
            stats: Application specific serving metrics.

        Returns:
            Model instance.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def respond(
        self,
        outcome: 'layout.Outcome',
        encoding: typing.Sequence['layout.Encoding'],
        scope: typing.Any,
    ) -> 'layout.Response':
        """Turn the application result into a native response to be passed back to the requestor.

        This involves assembling the result structure and encoding it into a native format.

        Args:
            outcome: Output to be returned.
            encoding: Accepted encoding media types.
            scope: Optional metadata carried over from :meth:`receive`.

        Returns:
            Encoded native response.
        """
        raise NotImplementedError()
