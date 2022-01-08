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
import pathlib
import typing

from forml.io import layout
from forml.runtime import asset

from . import _component


class Descriptor(abc.ABC):
    """Application descriptor."""

    @classmethod
    def load(cls, path: pathlib.Path) -> 'Descriptor':
        """Load the descriptor instance."""
        return _component.load(path.with_suffix('').name, path.parent)

    @abc.abstractmethod
    def decode(self, request: layout.Request) -> layout.Entry:
        """Decode the raw payload into a format accepted by the application."""

    @abc.abstractmethod
    def encode(self, result: layout.Result, encoding: typing.Sequence[layout.Encoding]) -> layout.Response:
        """Encode the application response into a raw bytes to be passed back by the engine."""

    @abc.abstractmethod
    def select(self, registry: asset.Registry, **kwargs) -> asset.Instance:
        """Select the model to be used for serving the request."""
