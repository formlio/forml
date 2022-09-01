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
ForML projects templating.
"""
import datetime
import getpass
import logging
import pathlib
import typing

import jinja2

import forml

from . import _conf

LOGGER = logging.getLogger(__name__)


def find(name: str) -> pathlib.Path:
    """Find the project template with the given name.

    The templates are searched for in the config locations (:data:`setup.USRDIR
    <forml.setup.USRDIR>` and :data:`setup.SYSDIR <forml.setup.SYSDIR>`.

    Args:
        name: Name of the template to be found (directory name).

    Returns:
        Path to the given template.

    Raises:
        forml.MissingError: If there is no template with that name.

    """
    for root in reversed(_conf.PATH):  # top priority first
        path = root / _conf.CONFIG[_conf.SECTION_TEMPLATING][_conf.OPT_PATH] / name
        if path.is_dir():
            return path
    raise forml.MissingError(f'Template {name} not found')


def generate(target: pathlib.Path, template: pathlib.Path, context: typing.Mapping[str, typing.Any]) -> None:
    """Generate a directory structure based on the given template.

    Args:
        target: Root directory of the generated structure.
        template: Source template directory.
        context: Jinja context to be used for the template interpolation.

    Raises:
        forml.InvalidError: If any of the target elements already exist.
    """

    def gendir(srcdir: pathlib.Path, dstdir: pathlib.Path) -> None:
        """Recursive generator of individual directory levels."""
        assert dstdir.is_dir(), f'Destination {dstdir} not a directory.'
        for src in srcdir.iterdir():
            srcname = env.from_string(src.name).render(context)
            *subdirs, dstname = srcname.split('.', srcname[: srcname.rfind('.py')].count('.'))
            dst = dstdir
            for sub in subdirs:
                dst /= sub
                dst.mkdir(parents=False, exist_ok=True)
            dst /= dstname
            if dst.suffix == '.jinja':
                dst = dst.with_suffix('')
            if dst.exists():
                raise forml.InvalidError(f'{dst} already exists.')
            dstmode = src.stat().st_mode
            if src.is_dir():
                dst.mkdir(parents=False, exist_ok=False, mode=dstmode)
                gendir(src, dst)
                continue
            assert src.is_file(), f'Source {src} not a file.'
            LOGGER.debug('Rendering %s as %s', src, dst)
            dst.touch(mode=dstmode, exist_ok=False)
            content = env.get_template(str(src.relative_to(template))).render(context)
            dst.write_text(content)

    target.mkdir(parents=True, exist_ok=True)
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template))
    gendir(template, target)


def project(
    name: str,
    path: pathlib.Path,
    template: typing.Optional[str],
    package: typing.Optional[str],
    version: typing.Optional[str],
    requirements: typing.Sequence[str],
) -> None:
    """Generate a new project structure based on the requested template.

    Args:
        name: New project name
        path: Project root directory.
        template: Name of a project template to use.
        package: Python package path to be used.
        version: Initial project version.
        requirements: List of project install dependencies.
    """
    context = {
        'forml': {'version': forml.__version__},
        'project': {
            'name': name,
            'package': package or name.replace('-', '_'),
            'version': version,
            'requirements': requirements,
        },
        'system': {'date': datetime.datetime.utcnow(), 'user': getpass.getuser()},
    }
    if path.exists():
        if not path.is_dir():
            raise forml.InvalidError(f'Target {path} is not a directory.')
        path /= name
    src = find(template or _conf.CONFIG[_conf.SECTION_TEMPLATING][_conf.OPT_DEFAULT])
    generate(path, src, context)
