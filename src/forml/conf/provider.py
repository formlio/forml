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
ForML provider configs.
"""
import types
import typing

from forml import conf
from forml.conf import section as secmod


class Section(secmod.Parsed):
    """Special sections of forml providers config options.
    """
    FIELDS: typing.Tuple[str] = ('name', 'kwargs')
    REFEREE: str = conf.SECTION_PLATFORM

    @classmethod
    def extract(cls, reference: str, *_) -> typing.Tuple[typing.Any]:
        kwargs = dict(super().extract(reference)[0])
        provider = kwargs.pop(conf.OPT_PROVIDER, reference)
        return str(provider), types.MappingProxyType(kwargs)

    def __hash__(self):
        return hash(self.__class__) ^ hash(self.name)  # pylint: disable=no-member

    def __eq__(self, other: 'Section'):
        return isinstance(other, self.__class__) and other.name == self.name  # pylint: disable=no-member

    def __lt__(self, other: 'Section') -> bool:
        return self.name < other.name  # pylint: disable=no-member


class Registry(secmod.Single, Section):
    """Registry provider.
    """
    SELECTOR = conf.OPT_REGISTRY


class Feed(Section):
    """Feed providers.
    """
    SELECTOR = conf.OPT_FEED
    FIELDS: typing.Tuple[str] = ('name', 'priority', 'kwargs')

    @classmethod
    def extract(cls, reference: str, *_) -> typing.Tuple[typing.Any]:
        name, kwargs = super().extract(reference)
        kwargs = dict(kwargs)
        priority = kwargs.pop(conf.OPT_PRIORITY, 0)
        return name, float(priority), types.MappingProxyType(kwargs)

    def __lt__(self, other: 'Feed') -> bool:
        # pylint: disable=no-member
        return super().__lt__(other) if self.priority == other.priority else self.priority < other.priority


class Runner(secmod.Single, Section):
    """Runner provider.
    """
    SELECTOR = conf.OPT_RUNNER


class Testing:
    """Providers specific to testing.
    """
    class Runner(secmod.Single, Section):
        """Runner provider.
        """
        REFEREE = conf.SECTION_TESTING
        SELECTOR = conf.OPT_RUNNER
