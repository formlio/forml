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
DSL structures.
"""

import typing

from . import frame
from . import kind as kindmod


class Field(typing.NamedTuple):
    """Schema field class."""

    kind: kindmod.Any
    name: typing.Optional[str] = None

    def renamed(self, name: typing.Optional[str]) -> 'Field':
        """Return copy of the field with the new name.

        Args:
            name: New name to be used.

        Returns:
            New Field instance.
        """
        return self if name == self.name else Field(self.kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table schema definitions.

    Note the meta class is actually going to turn it into an instance of frame.Table which itself has a ``.schema``
    attribute derived from this class and represented using dsl.Source.Schema.
    """
