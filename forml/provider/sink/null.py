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
Null writer sink implementation.
"""
import typing

from forml import io

if typing.TYPE_CHECKING:
    from forml.io import layout


class Sink(io.Sink, alias='null'):
    """Null sink with no real write action.

    It still returns the :class:`layout.Outcome <forml.io.layout.Outcome>` combo of schema and
    the payload.

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [SINK.blackhole]
        provider = "null"
    """

    class Writer(io.Sink.Writer):
        """Overridden writer."""

        @classmethod
        def write(cls, data: 'layout.Native', **kwargs: typing.Any) -> None:
            """Write is no-op."""
            return None
