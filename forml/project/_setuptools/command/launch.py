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
Custom setuptools commands for pipeline execution modes.
"""
import abc
import logging
import typing

from setuptools.command import test

from forml.conf.parsed import provider

if typing.TYPE_CHECKING:
    from forml import runtime

LOGGER = logging.getLogger(__name__)


class Mode(test.test, metaclass=abc.ABCMeta):
    """Development mode extra commands (based on the standard test mode)."""

    user_options = [
        ('runner=', 'R', 'runtime runner'),
        ('feed=', 'I', 'etl feed'),
        ('lower=', None, 'lower trainset ordinal'),
        ('upper=', None, 'upper trainset ordinal'),
    ]

    def initialize_options(self) -> None:
        """Init options."""
        super().initialize_options()
        self.runner: typing.Optional[str] = None
        self.feed: typing.Optional[str] = None
        self.lower: typing.Optional[str] = None
        self.upper: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options."""
        self.ensure_string_list('feed')

    def run_tests(self) -> None:
        """This is the original test command entry point - let's override it with our actions."""
        LOGGER.debug('%s: starting %s', self.distribution.get_name(), self.__class__.__name__.lower())
        launcher = self.distribution.artifact.launcher(
            provider.Runner.resolve(self.runner), provider.Feed.resolve(self.feed)
        )
        result = self.launch(launcher, lower=self.lower, upper=self.upper)
        if result is not None:
            print(result)

    @staticmethod
    @abc.abstractmethod
    def launch(launcher: 'runtime.Virtual.Handler', *args, **kwargs) -> typing.Any:
        """Executing the particular runner target.

        Args:
            launcher: Runner instance.
            *args: Optional args.
            **kwargs: Optional kwargs.

        Returns:
            Whatever runner response.
        """


class Train(Mode):
    """Development train mode."""

    description = 'trigger the development train mode'

    @staticmethod
    def launch(launcher: 'runtime.Virtual.Handler', *args, **kwargs) -> typing.Any:
        return launcher.train(*args, **kwargs)


class Tune(Mode):
    """Development tune mode."""

    description = 'trigger the development tune mode'

    @staticmethod
    def launch(launcher: 'runtime.Virtual.Handler', *args, **kwargs) -> typing.Any:
        raise NotImplementedError('Tune mode is not yet supported')


class Eval(Mode):
    """Development eval mode."""

    description = 'trigger the model evaluation mode'

    @staticmethod
    def launch(launcher: 'runtime.Virtual.Handler', *args, **kwargs) -> typing.Any:
        return launcher.eval(*args, **kwargs)
