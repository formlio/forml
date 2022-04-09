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

__all__ = ['get_encoder', 'get_decoder', 'Encoder', 'Decoder', 'Generic']


class Generic(project.Descriptor):
    """Generic application descriptor base class."""

    @classmethod
    def decode(cls, request: layout.Request) -> layout.Request.Decoded:
        """Decode using the internal bank of supported decoders."""
        return layout.Request.Decoded(get_decoder(request.encoding).loads(request.payload))

    @classmethod
    def encode(
        cls, outcome: layout.Outcome, encoding: typing.Sequence[layout.Encoding], scope: typing.Any
    ) -> layout.Response:
        """Encode using the internal bank of supported encoders."""
        encoder = get_encoder(*encoding)
        return layout.Response(encoder.dumps(outcome), encoder.encoding)

    @classmethod
    def select(cls, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        project = registry.get('helloworld.PACKAGE.manifest.name')
        for release in reversed(project.list()):
            try:
                generation = project.get(release).list().last
            except asset.Level.Listing.Empty:
                continue
            break
        else:
            raise asset.Level.Listing.Empty(f'No models available for {project.key}')
        return asset.Instance(
            registry=registry,
            project=project.key,
            release=release,  # pylint: disable=undefined-loop-variable
            generation=generation,
        )
