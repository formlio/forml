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
Rest gateway provider.
"""
import asyncio
import logging
import typing

import uvicorn
from starlette import applications, exceptions
from starlette import requests as reqmod
from starlette import responses as respmod
from starlette import routing

import forml
from forml import io, runtime, setup
from forml.io import asset, layout

LOGGER = logging.getLogger(__name__)


class Apply(routing.Route):
    """Application endpoint route."""

    PATH = '/{application:str}'
    DEFAULT_ENCODING = 'application/octet-stream'

    def __init__(self, handler: typing.Callable[[str, layout.Request], typing.Awaitable[layout.Response]]):
        super().__init__(self.PATH, self.__endpoint, methods=['POST'])
        self.__handler: typing.Callable[[str, layout.Request], typing.Awaitable[layout.Response]] = handler

    async def __endpoint(self, request: reqmod.Request) -> respmod.Response:
        """Route endpoint implementation.

        Args:
            request: Input instance.

        Returns:
            Output instance.
        """
        application = request.path_params['application']
        encoding = layout.Encoding.parse(request.headers.get('content-type', self.DEFAULT_ENCODING))[0]
        accept = request.headers.get('accept')
        if accept:
            accept = layout.Encoding.parse(accept)
        payload = await request.body()
        try:
            result = await self.__handler(application, layout.Request(payload, encoding, request.query_params, accept))
        except layout.Encoding.Unsupported as err:
            raise exceptions.HTTPException(status_code=415, detail=str(err))
        except forml.MissingError as err:
            raise exceptions.HTTPException(status_code=404, detail=str(err))
        return respmod.Response(result.payload, media_type=result.encoding.header)


class Stats(routing.Route):
    """Stats endpoint route."""

    PATH = '/stats'

    def __init__(self, handler: typing.Callable[[], typing.Awaitable[runtime.Stats]]):
        super().__init__(self.PATH, self.__endpoint, methods=['GET'])
        self.__handler: typing.Callable[[], typing.Awaitable[runtime.Stats]] = handler

    async def __endpoint(self, _: reqmod.Request) -> respmod.Response:
        """Route endpoint implementation.

        Returns:
            Output instance.
        """
        result = await self.__handler()
        return respmod.Response(str(result), media_type='text/plain')


class Gateway(runtime.Gateway, alias='rest'):
    """Gateway(inventory: typing.Optional[asset.Inventory] = None, registry: typing.Optional[asset.Registry] = None, feeds: typing.Optional[io.Importer] = None, processes: typing.Optional[int] = None, loop: typing.Optional[asyncio.AbstractEventLoop] = None, server: typing.Callable[[applications.Starlette, ...], None] = uvicorn.run, **options)

    Serving gateway implemented as a RESTful API.

    The frontend provides the following HTTP endpoints:

    ==================  ======  ==================================================================
    Path                Method  Description
    ==================  ======  ==================================================================
    ``/stats``          GET     Retrieve the Engine-provided performance :class:`metrics report
                                <forml.runtime.Stats>`.
    ``/<application>``  POST    Prediction request for the given :ref:`application <application>`.
                                The entire request *body* is passed to the :ref:`Engine <serving>`
                                as the :class:`layout.Request.payload <forml.io.layout.Request>`
                                with the declared ``content-type`` indicated via the ``.encoding``
                                and any potential query parameters bundled within the ``.params``.
    ==================  ======  ==================================================================

    Args:
        inventory: Inventory of applications to be served (default as per the platform
                   configuration).
        registry: Model registry of project artifacts to be served (default as per the platform
                  configuration).
        feeds: Feeds to be used for potential feature augmentation (default as per the platform
               configuration).
        processes: Process pool size for each model sandbox.
        loop: Explicit event loop instance.
        server: Serving loop main function accepting the provided `application instance
                <https://www.starlette.io/applications/>`_ (defaults to `uvicorn.run
                <https://www.uvicorn.org/deployment/#running-programmatically>`_).
        options: Additional serving loop keyword arguments (i.e. `Uvicorn settings
                 <https://www.uvicorn.org/settings/>`_).

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [GATEWAY.http]
        provider = "rest"
        port = 8080
        processes = 3

    Important:
        Select the ``rest`` :ref:`extras to install <install-extras>` ForML together with the
        Starlette/Uvicorn support.
    """  # pylint: disable=line-too-long  # noqa: E501

    setup.LOGGING.endswith = lambda _: False  # temporal Uvicorn workaround until PR #1716 is merged
    OPTIONS = {'headers': [('server', f'ForML {forml.__version__}')], 'log_config': setup.LOGGING}
    """Default server loop options."""

    def __init__(
        self,
        inventory: typing.Optional[asset.Inventory] = None,
        registry: typing.Optional[asset.Registry] = None,
        feeds: typing.Optional[io.Importer] = None,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
        server: typing.Callable[[applications.Starlette, ...], None] = uvicorn.run,
        **options,
    ):
        super().__init__(inventory, registry, feeds, processes=processes, loop=loop, server=server, options=options)

    @classmethod
    def run(
        cls,
        apply: typing.Callable[[str, layout.Request], typing.Awaitable[layout.Response]],
        stats: typing.Callable[[], typing.Awaitable[runtime.Stats]],
        **kwargs,
    ) -> None:
        routes = [Apply(apply), Stats(stats)]
        app = applications.Starlette(routes=routes, debug=False)
        kwargs['server'](app, **(cls.OPTIONS | kwargs['options']))
