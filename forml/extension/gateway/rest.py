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
from forml import io, runtime
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
        except forml.MissingError as err:
            raise exceptions.HTTPException(status_code=404, detail=str(err))
        return respmod.Response(result.payload, media_type=result.encoding.header)


class Stats(routing.Route):
    """Stats endpoint route."""

    PATH = '/stats'

    def __init__(self, handler: typing.Callable[[], typing.Awaitable[layout.Stats]]):
        super().__init__(self.PATH, self.__endpoint, methods=['GET'])
        self.__handler: typing.Callable[[], typing.Awaitable[layout.Stats]] = handler

    async def __endpoint(self, _: reqmod.Request) -> respmod.Response:
        """Route endpoint implementation.

        Returns:
            Output instance.
        """
        result = await self.__handler()
        return respmod.Response(str(result), media_type='text/plain')


class Gateway(runtime.Gateway, alias='rest'):
    """Rest frontend."""

    DEFAULTS = {'headers': [('server', f'ForML {forml.__version__}')]}

    def __init__(
        self,
        inventory: typing.Optional[asset.Inventory] = None,
        registry: typing.Optional[asset.Registry] = None,
        feeds: typing.Optional[io.Importer] = None,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
        server: typing.Callable[[applications.Starlette, ...], None] = uvicorn.run,
        **kwargs,
    ):
        super().__init__(inventory, registry, feeds, processes=processes, loop=loop)
        self._server: typing.Callable[[applications.Starlette, ...], None] = server
        self._kwargs = self.DEFAULTS | kwargs

    def run(
        self,
        apply: typing.Callable[[str, layout.Request], typing.Awaitable[layout.Response]],
        stats: typing.Callable[[], typing.Awaitable[layout.Stats]],
    ) -> None:
        routes = [Apply(apply), Stats(stats)]
        app = applications.Starlette(routes=routes, debug=True)
        self._server(app, **self._kwargs)
