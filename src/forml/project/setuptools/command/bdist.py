import os
import zipapp

import pip._internal as pip
import sys
import setuptools
import typing

from forml.project import distribution


class Package(setuptools.Command):
    """ForML build package.
    """
    description = 'create a ForML distribution'

    user_options = [
        ('bdist-dir=', 'b',  'temporary directory for creating the distribution'),
        ('interpreter=', None, 'python interpreter'),
        ('dist-dir=', 'd', 'directory to put final built distributions in'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        self.bdist_dir: typing.Optional[str] = None
        self.interpreter: str = f'/usr/bin/env python{sys.version_info.major}'
        self.dist_dir: typing.Optional[str] = None

    def finalize_options(self):
        """Fini options.
        """
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base, distribution.Package.FORMAT)

        need_options = ('dist_dir', )
        self.set_undefined_options('bdist', *zip(need_options, need_options))

    @property
    def dist_name(self) -> str:
        return f'{self.distribution.get_name()}-{self.distribution.get_version()}.{distribution.Package.FORMAT}'

    @property
    def manifest(self) -> distribution.Manifest:
        name = self.distribution.get_name()
        version = self.distribution.get_version()
        return distribution.Manifest(name=name, version=version, package='titanic')

    def run(self):

        pip.main(['install', '--upgrade', '--no-user', '--target', self.bdist_dir,
                  os.path.abspath(os.path.dirname(sys.argv[0]))])

        self.manifest.write(self.bdist_dir)
        if not os.path.exists(self.dist_dir):
            os.makedirs(self.dist_dir)
        target = os.path.join(self.dist_dir, self.dist_name)
        zipapp.create_archive(self.bdist_dir, target=target, interpreter=self.interpreter,
                              main='foo.asd:qwe',
                              compressed=True)
