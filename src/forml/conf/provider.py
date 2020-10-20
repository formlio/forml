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

from forml import conf, error
from forml.conf import section as secmod


class Meta(secmod.Meta):
    """Customized metaclass for providing the `path` property.
    """
    @property
    def path(cls) -> typing.Iterable[str]:
        """Getter for the configured search path of given provider.

        Returns: Sequence of search paths.
        """
        return conf.PARSER.get(cls.GROUP, {}).get(conf.OPT_PATH, [])


class Section(secmod.Parsed, metaclass=Meta):
    """Special sections of forml providers config options.
    """
    FIELDS: typing.Tuple[str] = 'reference', 'params'
    SELECTOR = conf.OPT_DEFAULT

    @classmethod
    def extract(cls, reference: str, *_) -> typing.Tuple[typing.Any]:
        params = dict(super().extract(reference)[0])
        provider = params.pop(conf.OPT_PROVIDER, reference)
        return str(provider), types.MappingProxyType(params)

    def __hash__(self):
        return hash(self.__class__) ^ hash(self.reference)  # pylint: disable=no-member

    def __eq__(self, other: 'Section'):
        return isinstance(other, self.__class__) and other.reference == self.reference  # pylint: disable=no-member

    def __lt__(self, other: 'Section') -> bool:
        return self.reference < other.reference  # pylint: disable=no-member


class Runner(secmod.Single, Section):
    """Runner provider.
    """
    INDEX: str = conf.SECTION_RUNNER
    GROUP: str = conf.SECTION_RUNNER


class Registry(secmod.Single, Section):
    """Registry provider.
    """
    INDEX: str = conf.SECTION_REGISTRY
    GROUP: str = conf.SECTION_REGISTRY


class Feed(Section):
    """Feed providers.
    """
    INDEX: str = conf.SECTION_FEED
    GROUP: str = conf.SECTION_FEED
    FIELDS: typing.Tuple[str] = 'reference', 'priority', 'params'

    @classmethod
    def extract(cls, reference: str, *_) -> typing.Tuple[typing.Any]:
        name, params = super().extract(reference)
        params = dict(params)
        priority = params.pop(conf.OPT_PRIORITY, 0)
        return name, float(priority), types.MappingProxyType(params)

    def __lt__(self, other: 'Feed') -> bool:
        # pylint: disable=no-member
        return super().__lt__(other) if self.priority == other.priority else self.priority < other.priority


class Sink(secmod.Single, Section):
    """Registry provider.
    """
    INDEX: str = conf.SECTION_SINK
    GROUP: str = conf.SECTION_SINK

    class Mode(metaclass=Meta):
        """Sink mode is a tuple of potentially different sinks selected for specific pipeline modes. It allows the
        [SINK] index section to provide alternative sink references for each modes as follows:

        [SINK]
        default = <default-sink-reference>
        train = <train-sink-reference>
        apply = <apply-sink-reference>
        eval = <eval-sink-reference>
        """
        FIELDS: typing.Tuple[str] = 'train', 'apply', 'eval'
        INDEX: str = conf.SECTION_SINK
        GROUP: str = conf.SECTION_SINK

        @classmethod
        def parse(cls, reference: typing.Optional[str] = None) -> 'Sink.Mode':
            """Parse the SINK section returning the tuple of sink configs for the particular modes.

            Args:
                reference: Optional sync reference - if provided, its used for all modes.

            Returns: Sink.Mode tuple with selected Sink config instances for the particular modes.
            """
            if not reference:
                try:
                    default = conf.PARSER[cls.INDEX].get(conf.OPT_DEFAULT)
                    train = conf.PARSER[cls.INDEX].get(conf.OPT_TRAIN, default)
                    apply = conf.PARSER[cls.INDEX].get(conf.OPT_APPLY, default)
                    evaluate = conf.PARSER[cls.INDEX].get(conf.OPT_EVAL, default)
                except KeyError as err:
                    raise error.Missing(f'Index section not found: [{cls.INDEX}]') from err
                if not train or not apply or not evaluate:
                    raise error.Missing(f'Missing default or explicit train/apply/eval sink references: [{cls.INDEX}]')
            else:
                train = apply = evaluate = reference
            return cls([Sink.parse(train), Sink.parse(apply), Sink.parse(evaluate)])
