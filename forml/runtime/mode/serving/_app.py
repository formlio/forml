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
import abc
import typing

from forml import _provider as provmod
from forml.runtime import asset


class Manifest(abc.ABC):
    """Application descriptor."""

    @abc.abstractmethod
    def decode(self, request: bytes, mime: str) -> typing.Any:
        """Decode the raw payload into a format accepted by the application."""

    @abc.abstractmethod
    def encode(self, response: typing.Any, mime: str) -> bytes:
        """Encode the application response into a raw bytes to be passed back by the engine."""

    @abc.abstractmethod
    def select(self, registry: typing.Optional[asset.Registry] = None, **kwargs) -> asset.Instance:
        """Select the model to be used for serving the request."""


class Inventory(provmod.Interface):
    """Application manifest storage."poklop0
