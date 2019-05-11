"""
Custom setuptools commands.
"""
import contextlib
import itertools
import logging
import os
import sys
import typing

import pkg_resources
import setuptools

from forml import project, etl
from forml.runtime.asset import access
from forml.runtime.asset.persistent.registry import devel as devreg
from forml.runtime import process

LOGGER = logging.getLogger(__name__)


class Train(setuptools.Command):
    """Development train mode.
    """
    description = 'trigger the development train mode'
    user_options = [
        ('runner=', None, 'runtime runner'),
        ('engine=', None, 'etl engine'),
        ('lower=', None, 'lower trainset ordinal'),
        ('upper=', None, 'upper trainset ordinal'),
    ]

    def initialize_options(self):
        """Init options.
        """
        self.runner: str = 'dask'
        self.engine: str = 'devel'
        self.lower: typing.Optional[str] = None
        self.upper: typing.Optional[str] = None

    def finalize_options(self):
        """Fini options.
        """

    @contextlib.contextmanager
    def project_on_sys_path(self):
        """Place the given project on the sys path.
        """
        self.run_command('egg_info')

        # Build extensions in-place
        self.reinitialize_command('build_ext', inplace=1)
        self.run_command('build_ext')

        ei_cmd = self.get_finalized_command("egg_info")

        old_path = sys.path[:]
        old_modules = sys.modules.copy()

        try:
            project_path = pkg_resources.normalize_path(ei_cmd.egg_base)
            sys.path.insert(0, project_path)
            pkg_resources.working_set.__init__()
            pkg_resources.add_activation_listener(lambda dist: dist.activate())  # pylint: disable=not-callable
            pkg_resources.require('%s==%s' % (ei_cmd.egg_name, ei_cmd.egg_version))
            with self.paths_on_pythonpath([project_path]):
                yield
        finally:
            sys.path[:] = old_path
            sys.modules.clear()
            sys.modules.update(old_modules)
            pkg_resources.working_set.__init__()

    @staticmethod
    @contextlib.contextmanager
    def paths_on_pythonpath(paths):
        """Add the indicated paths to the head of the PYTHONPATH environment variable so that subprocesses will also
        see the packages at these paths.

        Do this in a context that restores the value on exit.
        """
        nothing = object()
        orig_pythonpath = os.environ.get('PYTHONPATH', nothing)
        current_pythonpath = os.environ.get('PYTHONPATH', '')
        try:
            prefix = os.pathsep.join(paths)
            to_join = filter(None, [prefix, current_pythonpath])
            new_path = os.pathsep.join(to_join)
            if new_path:
                os.environ['PYTHONPATH'] = new_path
            yield
        finally:
            if orig_pythonpath is nothing:
                os.environ.pop('PYTHONPATH', None)
            else:
                os.environ['PYTHONPATH'] = orig_pythonpath

    @staticmethod
    def install_dists(dist):
        """Install the requirements indicated by self.distribution and return an iterable of the dists that were built.
        """
        ir_d = dist.fetch_build_eggs(dist.install_requires)
        tr_d = dist.fetch_build_eggs(dist.tests_require or [])
        er_d = dist.fetch_build_eggs(
            v for k, v in dist.extras_require.items()
            if k.startswith(':') and pkg_resources.evaluate_marker(k[1:])
        )
        return itertools.chain(ir_d, tr_d, er_d)

    @property
    def artifact(self) -> project.Artifact:
        """Get the artifact for this project.

        Returns: Artifact instance
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

    def run(self):
        """Command execution.
        """
        LOGGER.debug('%s: starting development training', self.distribution.name)
        with self.paths_on_pythonpath(d.location for d in self.install_dists(self.distribution)):
            with self.project_on_sys_path():
                runner = process.Runner[self.runner](etl.Engine[self.engine](), access.Assets(
                    devreg.Registry(self.artifact), self.distribution.name))
                runner.cvscore(lower=self.lower, upper=self.upper)
