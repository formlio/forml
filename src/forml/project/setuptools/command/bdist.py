import os

import pip
import sys
import setuptools
import typing


class Package(setuptools.Command):
    """ForML build package.
    """
    description = "create a ForML distribution"

    user_options = [
        ('bdist-dir=', 'd',  'temporary directory for creating the distribution'),
        ('interpreter=', None, 'python interpreter'),
        ('fat', None, 'python interpreter'),
    ]

    def initialize_options(self) -> None:
        """Init options.
        """
        super().initialize_options()
        self.bdist_dir: typing.Optional[str] = None
        self.interpreter: str = f'/usr/bin/env python{sys.version_info.major}'

    def finalize_options(self):
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base, 'mlp')

    def run(self):
        # pip install -r requirements.txt --target myapp
        # zipapp.create_archive(source, target=None, interpreter=None, main=None, filter=None, compressed=False)
        name = self.distribution.metadata.name
        version = self.distribution.metadata.version
        dependencies = self.distribution.metadata.install_requires