"""
Custom setuptools commands distribution packaging.
"""
import os
import sys
import typing

import setuptools

from forml.project import distribution


class Package(setuptools.Command):
    """ForML build package.
    """
    COMMAND = f'bdist_{distribution.Package.FORMAT}'
    description = 'create a ForML distribution'

    user_options = [
        ('bdist-dir=', 'b', 'temporary directory for creating the distribution'),
        ('dist-dir=', 'd', 'directory to put final built distributions in'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        self.bdist_dir: typing.Optional[str] = None
        self.dist_dir: typing.Optional[str] = None

    def finalize_options(self) -> None:
        """Fini options.
        """
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base, distribution.Package.FORMAT)

        need_options = ('dist_dir', )
        self.set_undefined_options('bdist', *zip(need_options, need_options))

    @property
    def filename(self) -> str:
        """Target package file name.
        """
        return f'{self.distribution.get_name()}-{self.distribution.get_version()}.{distribution.Package.FORMAT}'

    @property
    def manifest(self) -> distribution.Manifest:
        """Package manifest.
        """
        name = self.distribution.get_name()
        version = self.distribution.get_version()
        return distribution.Manifest(name=name, version=version, package='titanic')

    def run(self) -> None:
        """Trigger the packaging process.
        """
        import pip._internal as pip  # pylint: disable=import-outside-toplevel
        pip.main(['install', '--upgrade', '--no-user', '--target', self.bdist_dir,
                  os.path.abspath(os.path.dirname(sys.argv[0]))])
        if not os.path.exists(self.dist_dir):
            os.makedirs(self.dist_dir)
        target = os.path.join(self.dist_dir, self.filename)
        data = (self.COMMAND, '', str(distribution.Package.create(self.bdist_dir, self.manifest, target).path))
        if data not in self.distribution.dist_files:
            self.distribution.dist_files.append(data)
