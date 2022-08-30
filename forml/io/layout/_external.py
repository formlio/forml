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
External payload utilities.
"""
import collections
import re
import types
import typing

if typing.TYPE_CHECKING:
    from forml.io import dsl, layout


class Entry(typing.NamedTuple):
    """Internal representation of the decoded :class:`Request` payload."""

    schema: 'dsl.Source.Schema'
    data: 'layout.Tabular'


class Outcome(typing.NamedTuple):
    """Internal result payload representation to be encoded as :class:`Response`."""

    schema: 'dsl.Source.Schema'
    data: 'layout.RowMajor'


_CSV = re.compile(r'\s*,\s*')


class Request(collections.namedtuple('Request', 'payload, encoding, params, accept')):
    """Serving gateway request object.

    Args:
        payload: Raw encoded payload.
        encoding: Content type encoding instance.
        params: Optional application-level parameters.
        accept: Content types request for the eventual :class:`Response`.
    """

    class Decoded(typing.NamedTuple):
        """Decoded request case class."""

        entry: 'layout.Entry'
        """Input data points to be applied for prediction."""

        context: typing.Any = None
        """Custom (serializable!) metadata produced within the (user-defined) application scope
        and carried throughout the request processing flow."""

    payload: bytes
    """Encoded payload."""

    encoding: 'layout.Encoding'
    """Encoding media type."""

    params: typing.Mapping[str, typing.Any]
    """Optional application-level parameters."""

    accept: tuple['layout.Encoding']
    """Accepted response media type."""

    def __new__(
        cls,
        payload: bytes,
        encoding: 'layout.Encoding',
        params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        accept: typing.Optional[typing.Sequence['layout.Encoding']] = None,
    ):
        return super().__new__(
            cls, payload, encoding, types.MappingProxyType(dict(params or {})), tuple(accept or [encoding])
        )

    def __getnewargs__(self):
        return self.payload, self.encoding, dict(self.params), self.accept


class Response(typing.NamedTuple):
    """Serving gateway response object.

    Args:
        payload: Raw encoded payload.
        encoding: Content type encoding instance.
    """

    payload: bytes
    """Encoded payload."""

    encoding: 'layout.Encoding'
    """Encoding content type."""
