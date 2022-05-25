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
ForML application utils.
"""
import typing

from forml import project
from forml.io import asset, layout

from ._encoding import Decoder, Encoder, get_decoder, get_encoder
from ._strategy import Explicit, Latest, Selector

__all__ = ['get_encoder', 'get_decoder', 'Encoder', 'Explicit', 'Decoder', 'Generic', 'Latest', 'Selector']


class Generic(project.Descriptor):
    """Generic application descriptor."""

    def __init__(self, name: str, selector: typing.Optional[Selector] = None):
        self._name: str = name
        self._selector: Selector = selector or Latest(project=name)

    @property
    def name(self) -> str:
        return self._name

    def decode(self, request: layout.Request) -> layout.Request.Decoded:
        """Decode using the internal bank of supported decoders."""
        return layout.Request.Decoded(
            get_decoder(request.encoding).loads(request.payload), {'params': dict(request.params)}
        )

    def encode(
        self, outcome: layout.Outcome, encoding: typing.Sequence[layout.Encoding], scope: typing.Any
    ) -> layout.Response:
        """Encode using the internal bank of supported encoders."""
        encoder = get_encoder(*encoding)
        return layout.Response(encoder.dumps(outcome), encoder.encoding)

    def select(self, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        """Select using the provided selector."""
        return self._selector(registry, scope, stats)
