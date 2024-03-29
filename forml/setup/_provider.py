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
import typing

import forml

from . import _conf


class Meta(_conf.Meta):
    """Customized metaclass for providing the `path` property."""

    @property
    def path(cls) -> typing.Iterable[str]:
        """Getter for the configured search path of given provider.

        Returns:
            Sequence of search paths.
        """
        return _conf.CONFIG.get(cls.GROUP, {}).get(_conf.OPT_PATH, [])


class Provider(_conf.Section, metaclass=Meta):
    """Special sections of forml providers config options."""

    FIELDS: tuple[str] = ('reference', 'params')
    SELECTOR = _conf.OPT_DEFAULT

    @classmethod
    def _extract(
        cls, reference: str, kwargs: typing.Mapping[str, typing.Any]
    ) -> tuple[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]:
        kwargs = dict(kwargs)
        provider = kwargs.pop(_conf.OPT_PROVIDER, reference)
        _, kwargs = super()._extract(reference, kwargs)
        return [str(provider)], kwargs

    def __hash__(self):
        return hash(self.__class__) ^ hash(self.reference)  # pylint: disable=no-member

    def __eq__(self, other: 'Provider'):
        return isinstance(other, self.__class__) and other.reference == self.reference  # pylint: disable=no-member

    def __lt__(self, other: 'Provider') -> bool:
        return self.reference < other.reference  # pylint: disable=no-member


class Runner(Provider):
    """Runner provider."""

    INDEX: str = _conf.SECTION_RUNNER
    GROUP: str = _conf.SECTION_RUNNER


class Registry(Provider):
    """Registry provider."""

    INDEX: str = _conf.SECTION_REGISTRY
    GROUP: str = _conf.SECTION_REGISTRY


class Feed(_conf.Multi, Provider):
    """Feed providers."""

    INDEX: str = _conf.SECTION_FEED
    GROUP: str = _conf.SECTION_FEED
    FIELDS: tuple[str] = ('reference', 'priority', 'params')

    @classmethod
    def _extract(
        cls, reference: str, kwargs: typing.Mapping[str, typing.Any]
    ) -> tuple[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]:
        kwargs = dict(kwargs)
        priority = kwargs.pop(_conf.OPT_PRIORITY, 0)
        [reference], kwargs = super()._extract(reference, kwargs)
        return [reference, float(priority)], kwargs

    def __lt__(self, other: 'Feed') -> bool:
        # pylint: disable=no-member
        return super().__lt__(other) if self.priority == other.priority else self.priority < other.priority


class Sink(Provider):
    """Registry provider."""

    INDEX: str = _conf.SECTION_SINK
    GROUP: str = _conf.SECTION_SINK

    class Mode(metaclass=Meta):
        """Sink mode is a tuple of potentially different sinks selected for specific pipeline modes. It allows the
        [SINK] index section to provide alternative sink references for each modes as follows:

        [SINK]
        default = <default-sink-reference>
        apply = <apply-sink-reference>
        eval = <eval-sink-reference>
        """

        FIELDS: tuple[str] = ('apply', 'eval')
        INDEX: str = _conf.SECTION_SINK
        GROUP: str = _conf.SECTION_SINK

        @classmethod
        def resolve(cls, reference: typing.Optional[str] = None) -> 'Sink.Mode':
            """Parse the SINK section returning the tuple of sink configs for the particular modes.

            Args:
                reference: Optional sync reference - if provided, it is used for all modes.

            Returns:
                Sink.Mode tuple with selected Sink config instances for the particular modes.
            """
            if reference:
                apply = evaluate = reference
            else:
                try:
                    default = _conf.CONFIG[cls.INDEX].get(_conf.OPT_DEFAULT)
                    apply = _conf.CONFIG[cls.INDEX].get(_conf.OPT_APPLY, default)
                    evaluate = _conf.CONFIG[cls.INDEX].get(_conf.OPT_EVAL, default)
                except KeyError as err:
                    raise forml.MissingError(f'Index section not found: [{cls.INDEX}]') from err
                if not apply or not evaluate:
                    raise forml.MissingError(f'Missing default or explicit apply/eval sink references: [{cls.INDEX}]')
            return cls([Sink.resolve(apply), Sink.resolve(evaluate)])


class Inventory(Provider):
    """Inventory provider."""

    INDEX: str = _conf.SECTION_INVENTORY
    GROUP: str = _conf.SECTION_INVENTORY


class Gateway(Provider):
    """Gateway provider."""

    INDEX: str = _conf.SECTION_GATEWAY
    GROUP: str = _conf.SECTION_GATEWAY
