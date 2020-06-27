"""
Custom setuptools commands for pipeline execution modes.
"""
import abc
import logging
import os
import typing

from setuptools.command import test

from forml import etl
from forml.conf import provider
from forml.project import product
from forml.runtime import process

LOGGER = logging.getLogger(__name__)


class Mode(test.test, metaclass=abc.ABCMeta):
    """Development mode extra commands (based on the standard test mode).
    """
    user_options = [
        ('runner=', 'R', 'runtime runner'),
        ('feed=', 'I', 'etl feed'),
        ('lower=', None, 'lower trainset ordinal'),
        ('upper=', None, 'upper trainset ordinal'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        super().initialize_options()
        self.runner: typing.Optional[str] = None
        self.feed: typing.Optional[str] = None
        self.lower: typing.Optional[str] = None
        self.upper: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options.
        """

    @property
    def artifact(self) -> product.Artifact:
        """Get the artifact for this project.

        Returns: Artifact instance.
        """
        modules = dict(self.distribution.component)
        package = modules.pop('', None)
        if not package:
            for mod in modules.values():
                if '.' in mod:
                    package, _ = os.path.splitext(mod)
                    break
            else:
                package = self.distribution.packages[0]
        return product.Artifact(self.distribution.package_dir.get('', '.'), package=package, **modules)

    def run_tests(self) -> None:
        """This is the original test command entry point - lets override it with our actions.
        """
        LOGGER.debug('%s: starting %s', self.distribution.get_name(), self.__class__.__name__.lower())
        feed = provider.Feed.parse(self.feed)
        runner = provider.Runner.parse(self.runner)
        # pylint: disable=no-member
        launcher = self.artifact.launcher(process.Runner[runner.name], etl.Feed[feed.name](**feed.kwargs),
                                          **runner.kwargs)
        result = self.launch(launcher, lower=self.lower, upper=self.upper)
        if result is not None:
            print(result)

    @staticmethod
    @abc.abstractmethod
    def launch(runner: process.Runner, *args, **kwargs) -> typing.Any:
        """Executing the particular runner target.

        Args:
            runner: Runner instance.
            *args: Optional args.
            **kwargs: Optional kwargs.

        Returns: Whatever runner response.
        """


class Train(Mode):
    """Development train mode.
    """
    description = 'trigger the development train mode'
    launch = staticmethod(process.Runner.train)


class Tune(Mode):
    """Development tune mode.
    """
    description = 'trigger the development tune mode'

    @staticmethod
    def launch(runner: process.Runner, *args, **kwargs) -> typing.Any:
        raise NotImplementedError('Tune mode is not yet supported')


class Score(Mode):
    """Development score mode.
    """
    description = 'trigger the development score mode'
    launch = staticmethod(process.Runner.cvscore)
