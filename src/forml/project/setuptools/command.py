"""
Custom setuptools commands.
"""
import abc
import logging
import os
import typing

from setuptools.command import test

from forml import project, etl
from forml.runtime import process
from forml.runtime.asset import access
from forml.runtime.asset.persistent.registry import devel as devreg

LOGGER = logging.getLogger(__name__)


class Mode(test.test, metaclass=abc.ABCMeta):
    """Development mode extra commands (based on the standard test mode).
    """
    user_options = [
        ('runner=', None, 'runtime runner'),
        ('engine=', None, 'etl engine'),
        ('lower=', None, 'lower trainset ordinal'),
        ('upper=', None, 'upper trainset ordinal'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        super().initialize_options()
        self.runner: str = 'dask'
        self.engine: str = 'devel'
        self.lower: typing.Optional[str] = None
        self.upper: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options.

        Overriding to bypass the parent actions.
        """

    @property
    def artifact(self) -> project.Artifact:
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
        return project.Artifact(package=package, **modules)

    def run_tests(self) -> None:
        """This is the original test command entry point - lets override it with our actions.
        """
        LOGGER.debug('%s: starting %s', self.distribution.name, self.__class__.__name__.lower())
        runner = process.Runner[self.runner](etl.Engine[self.engine](), access.Assets(
            devreg.Registry(self.artifact), self.distribution.name))
        result = self.launch(runner, lower=self.lower, upper=self.upper)
        if result:
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


class Score(Mode):
    """Development score mode.
    """
    description = 'trigger the development score mode'
    launch = staticmethod(process.Runner.cvscore)
