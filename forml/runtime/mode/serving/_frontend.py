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
import collections
import typing

from forml.runtime import facility


class Request(collections.namedtuple('Request', 'payload, mime, accept')):
    payload: bytes
    mime: str
    accept: tuple[str]

    def __new__(cls, payload: bytes, mime: str, accept: typing.Optional[typing.Iterable[str]] = None):
        return super().__new__(cls, payload, mime, tuple(accept or [mime]))


class Response(typing.NamedTuple):
    payload: bytes
    mime: str


class Engine:
    def __init__(self, runner: facility.Runner):
        self._roster = ...
        self._manifests: dict[str] = dict()
        self._runner: facility.Runner = runner

    async def apply(self, app: str, request: Request) -> Response:
        """Engine predict entrypoint."""
