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
System instructions.
"""
import logging
import typing
import uuid

import forml

from .. import target

if typing.TYPE_CHECKING:
    from forml.io import asset


LOGGER = logging.getLogger(__name__)


class Loader(target.Instruction):
    """Registry based state loader."""

    def __init__(self, assets: 'asset.State', key: typing.Union[int, uuid.UUID]):
        self._assets: 'asset.State' = assets
        self._key: typing.Union[int, uuid.UUID] = key

    def execute(self) -> typing.Optional[bytes]:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Returns:
            Loaded state.
        """
        try:
            return self._assets.load(self._key)
        except forml.MissingError:
            LOGGER.warning('No previous generations found - node #%d defaults to no state', self._key)
            return None


class Dumper(target.Instruction):
    """Registry based state dumper."""

    def __init__(self, assets: 'asset.State'):
        self._assets: 'asset.State' = assets

    def execute(self, state: bytes) -> uuid.UUID:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Args:
            state: State to be persisted.

        Returns:
            Absolute state id.
        """
        return self._assets.dump(state)


class Getter(target.Instruction):
    """Extracting single item from a vector."""

    def __init__(self, index: int):
        self._index: int = index

    @property
    def index(self) -> int:
        """Index getter.

        Returns:
            Return the getter index value.
        """
        return self._index

    def __repr__(self):
        return super().__repr__() + f'#{self._index}'

    def execute(self, sequence: typing.Sequence[typing.Any]) -> typing.Any:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Args:
            sequence: Sequence of output arguments.

        Returns:
            Single output item.
        """
        return sequence[self._index]


class Committer(target.Instruction):
    """Commit a new release generation."""

    def __init__(self, assets: 'asset.State'):
        self._assets: 'asset.State' = assets

    def execute(self, *states: uuid.UUID) -> None:
        """Instruction functionality.

        Args:
            *states: Sequence of state IDs.
        """
        self._assets.commit(states)
