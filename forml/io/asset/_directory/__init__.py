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
Generic assets directory.
"""

import abc
import functools
import logging
import typing

import forml

if typing.TYPE_CHECKING:
    from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Level(metaclass=abc.ABCMeta):
    """Abstract directory level."""

    class Invalid(forml.InvalidError):
        """Indication of an invalid level."""

    class Key(metaclass=abc.ABCMeta):
        """Level key type."""

        class Invalid(forml.InvalidError, TypeError):
            """Invalid key type."""

        @property
        def next(self) -> 'Level.Key':
            """Get the next key in sequence.

            Returns:
                Next key.
            """
            raise NotImplementedError(f'Next key not supported for {self.__class__}')

    class Listing(tuple):
        """Helper class representing a registry listing."""

        class Empty(forml.MissingError):
            """Exception indicating empty listing."""

        def __new__(cls, items: typing.Iterable['Level.Key']):
            return super().__new__(cls, tuple(sorted(set(items))))

        @property
        def last(self) -> 'Level.Key':
            """Get the last (most recent) item from the listing.

            Returns:
                Id of the last item.
            """
            try:
                return self[-1]
            except IndexError as err:
                raise self.Empty('Empty listing') from err

    def __init__(self, key: typing.Any = None, parent: typing.Optional['Level'] = None):
        if key is not None:
            key = self.Key(key)
        self._key: typing.Optional['Level.Key'] = key
        self._parent: typing.Optional[Level] = parent

    def __repr__(self):
        return f'{self._parent}-{self.key}'

    def __hash__(self):
        return hash(self._parent) ^ hash(self.key)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other._parent == self._parent and other.key == self.key

    @property
    def registry(self) -> 'asset.Registry':
        """Registry instance.

        Returns:
            Registry instance.
        """
        return self._parent.registry

    @property
    def key(self) -> 'Level.Key':
        """Either user specified or last lazily listed level key.

        Returns:
            ID of this level.
        """
        if self._key is None:
            if not self._parent:
                raise ValueError('Parent or key required')
            LOGGER.debug("Determining implicit self key as parent's last listing")
            self._key = self._parent.list().last
        if self._parent and self._key not in self._parent.list():
            raise Level.Invalid(f'Invalid level key {self._key}')
        return self._key

    @abc.abstractmethod
    def list(self) -> 'Level.Listing':
        """Return the listing of this level.

        Returns:
            Level listing.
        """

    @abc.abstractmethod
    def get(self, key: 'Level.Key') -> 'Level':
        """Get an item from this level.

        Args:
            key: Item key to get.

        Returns:
            Item as a level instance.
        """


class Cache:
    """Helper for caching registry method calls."""

    def __init__(self, method: typing.Callable):
        self._method: str = method.__name__

    def __repr__(self):
        return repr(self.info)

    @functools.cache
    def __call__(self, registry: 'asset.Registry', *args, **kwargs):
        return getattr(registry, self._method)(*args, **kwargs)

    def clear(self) -> None:
        """Clear the cache."""
        self.__call__.cache_clear()  # pylint: disable=no-member

    @property
    def info(self) -> functools._CacheInfo:  # pylint: disable=protected-access
        """Return the cache info.

        Returns:
            Cache info tuple.
        """
        return self.__call__.cache_info()  # pylint: disable=no-member
