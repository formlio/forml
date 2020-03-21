"""
Custom setuptools commands distribution publishing.
"""
import typing

import setuptools
from forml.runtime.asset.directory import root

from forml import error, conf
from forml.project import distribution
from forml.project.setuptools.command import bdist
from forml.runtime.asset import persistent


class Registry(setuptools.Command):
    """ForML publish package.
    """
    description = 'publish a ForML distribution'

    user_options = [
        ('registry=', None, 'persistent registry to deploy to'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        self.registry: typing.Optional[conf.Registry] = conf.REGISTRY

    def finalize_options(self) -> None:
        """Fini options.
        """
        if isinstance(self.registry, str):
            self.registry = conf.Registry.parse(self.registry)

    def run(self) -> None:
        """Trigger the deployment process.
        """
        packages = [distribution.Package(f) for c, _, f in self.distribution.dist_files if c == bdist.Package.COMMAND]
        if not packages:
            raise error.Invalid('Must create and upload files in one command '
                                f'(e.g. setup.py {bdist.Package.COMMAND} upload)')
        project = self.distribution.get_name()
        for pkg in packages:
            root.Level(persistent.Registry[self.registry.name](**self.registry.kwargs)).get(project).put(pkg)
