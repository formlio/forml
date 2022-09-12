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
import logging
import pathlib
import typing

import forml
from forml import setup as setupmod
from forml.io import layout as laymod

from . import _strategy

if typing.TYPE_CHECKING:
    from forml import application, runtime
    from forml.io import asset, layout  # pylint: disable=reimported


LOGGER = logging.getLogger(__name__)


def setup(descriptor: 'application.Descriptor') -> None:  # pylint: disable=unused-argument
    """Interface for registering application descriptor instances.

    This function is expected to be called exactly once from within the application descriptor file.

    The true implementation of this function is only provided when imported within the *application
    loader context* (outside the context this is effectively no-op).

    Args:
        descriptor: Application descriptor instance.

    Examples:
        .. code-block:: python
           :caption: titanic.py

            from forml import application

            APP = application.Generic('forml-example-titanic')
            application.setup(APP)
    """
    LOGGER.debug('Application setup attempted outside of a loader context: %s', descriptor)


class Descriptor(abc.ABC):
    """Application descriptor abstract base class.

    The *serving layer* is using Application descriptors to control the query processing.

    Active descriptors are deployed through :class:`asset.Inventory <forml.io.asset.Inventory>`
    used by the *serving engine*.
    """

    class Handle(collections.namedtuple('Handle', 'path, descriptor')):
        """Descriptor handle referring to a module file containing the application descriptor
        instance.

        Args:
            path: File system path to the descriptor module file location.

        Raises:
            forml.MissingError: If the path does not exist.
            forml.InvalidError: If the path does not refer to a valid application descriptor.
        """

        path: pathlib.Path
        """File system path to the module containing the descriptor."""
        descriptor: 'application.Descriptor'
        """Actual descriptor."""

        def __new__(cls, path: typing.Union[str, pathlib.Path]):
            path = pathlib.Path(path).resolve()
            if not path.exists():
                raise forml.MissingError(f'Descriptor module not found: {path}')
            if not path.is_file():
                raise forml.InvalidError(f'Invalid descriptor module (file expected): {path}')
            module = inspect.getmodulename(path)
            if not module:
                raise forml.InvalidError(f'Invalid descriptor module (not a module): {path}')
            descriptor = setupmod.load(module, setup, path.parent)
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
            Application name is expected to be globally unique. This name will be used to register
            the application when publishing and to target it when serving.

        Returns:
            Application name.
        """

    @abc.abstractmethod
    def receive(self, request: 'layout.Request') -> 'layout.Request.Decoded':
        """Receive the raw payload and turn it into a structure suitable for predicting.

        This involves at least payload decoding plus potentially also any further data compilation
        necessary for prediction. Additionally, it might also produce custom metadata representing
        an *application context* to be passed down the chain all the way to :meth:`select` and
        :meth:`respond`.

        Args:
            request: Native request format.

        Returns:
            Decoded entry (adjusted for prediction) with optional custom (serializable!) context.

        Raises:
            layout.Encoding.Unsupported: If the received encoding is not supported.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def select(
        self,
        registry: 'asset.Directory',
        context: typing.Any,
        stats: 'runtime.Stats',
    ) -> 'asset.Instance':
        """Select the model instance to be used for serving the request.

        This can implement an arbitrary model-selection strategy with the use of the provided
        information.

        Args:
            registry: Model registry to select the model from.
            context: Optional metadata carried over from :meth:`receive`.
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
        context: typing.Any,
    ) -> 'layout.Response':
        """Turn the application result into a native response to be passed back to the requester.

        This involves assembling the resulting structure and encoding it into a native format.

        Args:
            outcome: Result to be returned.
            encoding: Accepted encoding media types.
            context: Optional metadata carried over from :meth:`receive`.

        Returns:
            Encoded native response.

        Raises:
            layout.Encoding.Unsupported: If none of the accepted encodings is supported.
        """
        raise NotImplementedError()


class Generic(Descriptor):
    """Generic application descriptor for basic serving scenarios.

    It simply runs the directly decoded (using the :func:`available decoders
    <forml.io.layout.get_decoder>`) request payload through the model/generation selected using
    the provided :class:`application.Selector <forml.application.Selector>` and returns the directly
    encoded (using the :func:`available encoders <forml.io.layout.get_encoder>`) outcomes as the
    response.

    Args:
        name: The (unique) name for this application registration/lookup.
        selector: Implementation of a particular model-selection strategy (defaults to
                  :class:`application.Latest <forml.application.Latest>` selector expecting the
                  project name to be *matching* the application name).

    Examples:
        >>> APP = application.Generic('forml-example-titanic')
    """

    def __init__(self, name: str, selector: typing.Optional['application.Selector'] = None):
        self._name: str = name
        self._strategy: 'application.Selector' = selector or _strategy.Latest(project=name)

    @property
    def name(self) -> str:
        return self._name

    def receive(self, request: 'layout.Request') -> 'layout.Request.Decoded':
        """Decode using the internal bank of supported decoders."""
        return laymod.Request.Decoded(
            laymod.get_decoder(request.encoding).loads(request.payload), {'params': dict(request.params)}
        )

    def respond(
        self, outcome: 'layout.Outcome', encoding: typing.Sequence['layout.Encoding'], context: typing.Any
    ) -> 'layout.Response':
        """Encode using the internal bank of supported encoders."""
        encoder = laymod.get_encoder(*encoding)
        return laymod.Response(encoder.dumps(outcome), encoder.encoding)

    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        """Select using the provided selector."""
        return self._strategy.select(registry, context, stats)
