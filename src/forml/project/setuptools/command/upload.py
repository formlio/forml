"""
Custom setuptools commands distribution publishing.
"""
import typing

import setuptools

from forml import error
from forml.conf import provider as provcfg
from forml.project import distribution
from forml.project.setuptools.command import bdist
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root


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
        self.registry: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options.
        """

    def run(self) -> None:
        """Trigger the deployment process.
        """
        packages = [distribution.Package(f) for c, _, f in self.distribution.dist_files if c == bdist.Package.COMMAND]
        if not packages:
            raise error.Invalid('Must create and upload files in one command '
                                f'(e.g. setup.py {bdist.Package.COMMAND} upload)')
        project = self.distribution.get_name()
        registry = provcfg.Registry.parse(self.registry)
        for pkg in packages:
            root.Level(persistent.Registry[registry.name](**registry.kwargs)).get(project).put(pkg)
